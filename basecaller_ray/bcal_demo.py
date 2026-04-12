import os, re, time, subprocess, tempfile
import csv, json, io
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo
from pathlib import Path
import ray
from ray.util.queue import Queue
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from google.cloud import storage

DORADO_BIN = os.environ.get("DORADO_BIN", os.path.expanduser("~/dorado-1.4.0-linux-x64/bin/dorado"))
MODELS_DIR = os.environ.get("DORADO_MODELS_DIR", os.path.expanduser("~/dorado_models"))
MODEL_NAME = "dna_r10.4.1_e8.2_400bps_fast@v5.0.0" #dna_r10.4.1_e8.2_400bps_hac@v5.0.0 # dna_r10.4.1_e8.2_400bps_fast@v5.0.0"

BC_PROG_THRESH = 90.0  # if any task is >= this % done, wait instead of launching new node
BATCH_SIZE = 10 # max files per GPU task
BATCH_TIMEOUT_SECS = 300 # dispatch partial batch after this many seconds
IDLE_TEARDOWN_SECS = 600 # match idle_timeout_minutes: 10 in bcal_startup_v5.yaml

def ts():
    return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M:%S EST")

# collect files from gcs
def uploaded_blobs_list(bucket_name, prefix=""):
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    return [blob.name for blob in blobs if not blob.name.endswith("/")]

# ensure necessary buckets exist
def ensure_prefix(bucket, prefix):
    blob = bucket.blob(prefix)
    if not blob.exists():
        blob.upload_from_string("")

@ray.remote(num_cpus=0, resources={"head_slot": 0.001})
class ProgressTracker:
    """Tracks per-task completion percentage. Workers report here; scheduler reads here."""
    def __init__(self):
        self._progress = {}  # blob_name -> float (0-100)

    def update(self, blob_name, percentage):
        self._progress[blob_name] = percentage

    def remove(self, blob_name):
        self._progress.pop(blob_name, None)

    def max_progress(self):
        return max(self._progress.values(), default=0.0)

    # coutns how many in-flight processing tasks are at or above threshold
    def count_above(self, threshold):
        return sum(1 for percentage in self._progress.values() if percentage >= threshold)

@ray.remote(num_cpus=0, resources={"head_slot": 0.001})
class GCSWatcher:
    def __init__(self, bucket_name, prefix="input_files/", poll_secs=30.0):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.poll_secs = poll_secs
        self.stopped = False

    def run(self, q):
        while not self.stopped:
            for blob_name in self.poll_new_blobs(): # moves uploaded/ -> pending/
                q.put(blob_name)
                print(f"Enqueued {blob_name} to q for processing.")
            time.sleep(self.poll_secs)

    def poll_new_blobs(self):
        client = storage.Client()
        bucket = client.bucket(self.bucket_name)
        for prefix in ["input_files/", "pending_files/", "processed_files/"]:
            ensure_prefix(bucket, prefix)
        new_blobs = uploaded_blobs_list(self.bucket_name, self.prefix)
        pending_blobs = []

        # change prefix to avoid re-polling same blob
        for blob_name in new_blobs:
            if blob_name.startswith("input_files/"):
                blob = bucket.blob(blob_name)
                new_name = "pending_files/" + blob_name[len("input_files/"):]
                bucket.rename_blob(blob, new_name)
                pending_blobs.append(new_name)
                print(f"Moved {blob_name} to {new_name}")

        return pending_blobs # "pending_files/..."

    def stop(self):
        self.stopped = True

@ray.remote(num_cpus=0, resources={"head_slot": 0.001})
class HeadScheduler:
    def __init__(self, bucket_name, max_in_flight=64):
        self.bucket_name = bucket_name
        self.max_in_flight = max_in_flight
        self.in_flight = set()
        self.near_done_reserved = 0  # files already queued to reuse near-done GPUs across ticks
        self.pending_batch = [] # files accumulating toward next batch
        self.batch_start_time = None # when first file in current batch arrived
        self.completed_batches = [] # results from finished tasks
        self._task_to_node = {} # ObjectRef -> node_id (targeted node, for bin-packing preference)
        self._node_last_active = {} # node_id -> timestamp of last task submission (for idle tiebreaking)

    def _drain_queue(self, q):
        """Pull files from q into pending_batch up to BATCH_SIZE."""
        while not q.empty() and len(self.pending_batch) < BATCH_SIZE:
            blob_name = q.get()
            self.pending_batch.append(blob_name)
            if self.batch_start_time is None:
                self.batch_start_time = time.time()

    def _batch_ready(self):
        if not self.pending_batch:
            return False
        if len(self.pending_batch) >= BATCH_SIZE:
            return True
        elapsed = time.time() - self.batch_start_time
        if elapsed >= BATCH_TIMEOUT_SECS:
            print(f"Batch timeout: dispatching {len(self.pending_batch)} files after {elapsed:.0f}s")
            return True
        return False

    def tick(self, q, tracker):
        # clear finished — each finished task frees one GPU, consume a reservation if one was held
        if self.in_flight:
            done, pending = ray.wait(list(self.in_flight), timeout=0, num_returns=len(self.in_flight))
            for ref in done:
                try:
                    result = ray.get(ref)
                    self.completed_batches.append(result)
                    actual_node_id = result.get("node_id")
                    if actual_node_id:
                        self._node_last_active[actual_node_id] = time.time()
                except Exception as e:
                    print(f"Task failed, not recorded in summary: {e}")
                self._task_to_node.pop(ref, None)
                del ref  # HAN NOTE: release object store pin so Ray can scale down idle workers
            finished_count = len(done)
            done.clear() # HAN NOTE: this is needed to avoid a memory leak in ray.wait's done list
            self.near_done_reserved = max(0, self.near_done_reserved - finished_count)
            self.in_flight = set(pending)

        self._drain_queue(q)

        # fetch near-done count once per tick (only when needed)
        near_done_slots = None
        submitted_to_near_done = 0

        while self._batch_ready() and len(self.in_flight) < self.max_in_flight:
            free_gpus = ray.available_resources().get("GPU", 0)
            if free_gpus == 0:
                if near_done_slots is None:
                    near_done_slots = ray.get(tracker.count_above.remote(BC_PROG_THRESH))
                    # subtract reservations already made in prior ticks
                    available_near_done = max(0, near_done_slots - self.near_done_reserved)
                    print(f"All GPUs busy — {near_done_slots} near-done, {self.near_done_reserved} already reserved, {available_near_done} slots free")
                if submitted_to_near_done < available_near_done:
                    # this batch will reuse a GPU that's about to free up — no new node needed
                    submitted_to_near_done += 1
                    self.near_done_reserved += 1
                    is_near_done_submission = True
                else:
                    # no free GPUs and no near-done slots — submit anyway to trigger autoscaler
                    print(f"No free GPUs — submitting batch to trigger autoscaler node launch")
                    is_near_done_submission = False
            else:
                is_near_done_submission = False

            batch = self.pending_batch[:BATCH_SIZE]
            self.pending_batch = self.pending_batch[BATCH_SIZE:]
            self.batch_start_time = time.time() if self.pending_batch else None

            # Bin-packing with idle-aware tiebreaking:
            # 1. prefer nodes with fewest free GPUs (already running tasks) so idle nodes can de-scale
            # 2. among nodes with equal free GPUs (eg all idle), prefer the most recently active node so idle timers on other nodes are not reset unnecessarily.
            node_task_counts = Counter(v for v in self._task_to_node.values() if v is not None)
            gpu_nodes = [n for n in ray.nodes() if n["Alive"] and n.get("Resources", {}).get("GPU", 0) > 0]
            def node_sort_key(n):
                total = n.get("Resources", {}).get("GPU", 0)
                free = max(0, total - node_task_counts.get(n["NodeID"], 0))
                # Negate last_active so more recent timestamps sort smaller (preferred by min())
                last_active = self._node_last_active.get(n["NodeID"], 0.0)
                return (free, -last_active)
            candidates = [n for n in gpu_nodes if max(0, n.get("Resources", {}).get("GPU", 0) - node_task_counts.get(n["NodeID"], 0)) > 0]
            if candidates:
                target_node = min(candidates, key=node_sort_key)
            elif is_near_done_submission:
                target_node = min(gpu_nodes, key=node_sort_key, default=None)
            else:
                target_node = None
            strategy = NodeAffinitySchedulingStrategy(target_node["NodeID"], soft=True) if target_node else "DEFAULT"

            print(f"Scheduling batch of {len(batch)} files (in_flight={len(self.in_flight)}, free_gpu={free_gpus}, target_node={target_node['NodeID'][:8] if target_node else 'none'})")
            ref = pod5_basecalling.options(scheduling_strategy=strategy).remote(self.bucket_name, batch, tracker)
            self._task_to_node[ref] = target_node["NodeID"] if target_node else None
            if target_node:
                self._node_last_active[target_node["NodeID"]] = time.time()
            self.in_flight.add(ref)

            self._drain_queue(q)

        return len(self.in_flight)

    def get_completed_batches(self):
        return list(self.completed_batches)

@ray.remote(num_gpus=1, num_cpus=0, max_calls=1) # max_calls = 1
def pod5_basecalling(bucket_name: str, blob_names: list, tracker):
    for blob_name in blob_names:
        if not blob_name.startswith("pending_files/"):
            raise ValueError(f"{blob_name} must start with pending_files/")

    batch_key = blob_names[0]  # use first blob name as tracker key for this batch
    print(f"Processing batch of {len(blob_names)} files on Ray task with GPU...")

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Work in a per-task temp directory
    # with tempfile.TemporaryDirectory(prefix="dorado_") as workdir:
    with tempfile.TemporaryDirectory(prefix="dorado_", dir="/data") as workdir:
        workdir = Path(workdir)
        pod5_dir = workdir / "pod5_inputs"
        pod5_dir.mkdir()
        out_dir = workdir / "output"
        out_dir.mkdir()

        batch_t0 = time.time()

        # Download all POD5 files into pod5_dir in parallel
        def _download(blob_name):
            dest = pod5_dir / Path(blob_name).name
            client.bucket(bucket_name).blob(blob_name).download_to_filename(str(dest))
            print(f"Downloaded {blob_name} to {dest}")

        t_download_start = time.time()
        with ThreadPoolExecutor(max_workers=len(blob_names)) as pool:
            futures = {pool.submit(_download, b): b for b in blob_names}
            for f in as_completed(futures):
                f.result()  # re-raises any download exception
        t_download_end = time.time()

        total_size_gb = round(sum(f.stat().st_size for f in pod5_dir.glob("*.pod5")) / 1e9, 2)

        # Count total reads across all files for progress tracking
        total_reads = 0
        try:
            for pod5_path in pod5_dir.glob("*.pod5"):
                ids_tsv = workdir / f"{pod5_path.stem}_read_ids.tsv"
                subprocess.run(
                    ["pod5", "view", str(pod5_path), "--include", "read_id", "--output", str(ids_tsv)],
                    check=True, stderr=subprocess.DEVNULL
                )
                n = max(0, int(subprocess.check_output(["wc", "-l", str(ids_tsv)]).split()[0]) - 1)
                total_reads += n
                print(f"  {pod5_path.name}: {n} reads")
            print(f"Total reads across batch: {total_reads}")
        except Exception as e:
            print(f"Could not count reads (progress tracking disabled): {e}")

        # Run Dorado on the whole directory — one model load for all files
        summary_path = out_dir / "sequencing_summary.txt"
        print(f"Summary path for progress tracking is {summary_path}")
        cmd = [
            DORADO_BIN,
            "basecaller",
            "--models-directory", MODELS_DIR,
            "--batchsize", "5120", # V100-16GB + fast@v5.0.0 optimal; skips ~90s GPU benchmarking
            MODEL_NAME,
            str(pod5_dir), # directory input — Dorado processes all pod5s in one pass
            "--output-dir", str(out_dir),
            "--emit-summary",
        ]
        print(f"[{ts()}] Running Dorado: {' '.join(cmd)}")
        print(f"batch_key={batch_key}: total_reads={total_reads}, summary_exists={summary_path.exists()}")

        stderr_log = workdir / "dorado_stderr.log"
        with open(stderr_log, "wb") as stderr_fh:
            t_dorado_start = time.time()
            proc = subprocess.Popen(cmd, stderr=stderr_fh)

            while proc.poll() is None: # returns None if dorado is still running

                # TODO can get rid of this after debugging
                if not (total_reads > 0 and summary_path.exists()):
                    print(f"Progress skipped: total_reads={total_reads}, summary_exists={summary_path.exists()}")

                if total_reads > 0 and summary_path.exists():
                    try:
                        done = sum(1 for _ in open(summary_path)) - 1  # subtract header line
                        percentage = 100.0 * max(0, done) / total_reads
                        tracker.update.remote(batch_key, percentage)
                        print(f"Progress batch [{batch_key}]: {percentage:.1f}% ({done}/{total_reads} reads)")
                    except Exception as e:
                        print(f"Error occurred while updating progress for batch [{batch_key}]: {e}")
                time.sleep(10) # how often to calculate progress percentage

                # print last few lines of stderr so we can see Dorado's status
                try:
                    lines = stderr_log.read_bytes().decode(errors="replace").splitlines()
                    print(f"[dorado stderr tail] {lines[-3:] if lines else 'empty'}")
                except Exception:
                    pass

            t_dorado_end = time.time()

        print(f"[{ts()}] Dorado finished for batch [{batch_key}] with exit code {proc.returncode}")

        stderr_output = stderr_log.read_bytes()
        tracker.remove.remote(batch_key)

        if proc.returncode != 0:
            print(f"Dorado failed (exit {proc.returncode}):\n{stderr_output.decode()}", flush=True)
            raise subprocess.CalledProcessError(proc.returncode, cmd)

        # 4) Find all output BAMs
        print(f"Searching for BAMs in {out_dir}, contents: {list(out_dir.rglob('*'))}")
        bam_files = list(out_dir.glob("**/*.bam"))
        if not bam_files:
            msg = f"No BAM file found in {out_dir}, contents: {list(out_dir.rglob('*'))}"
            print(msg, flush=True)
            raise RuntimeError(msg)

        # upload all BAMs — prefix filename with batch_key stem to avoid collisions.
        # batch_prefix = Path(batch_key).stem
        # for out_bam in bam_files:
        #     out_gcs = f"generated_data/{batch_prefix}_{out_bam.stem}.calls.bam"
        #     print(f"[{ts()}] Uploading BAM to gs://{bucket_name}/{out_gcs}")
        #     bucket.blob(out_gcs).upload_from_filename(str(out_bam))
        #     print(f"[{ts()}] Uploaded BAM to gs://{bucket_name}/{out_gcs}")
        #     uploaded_bams.append(out_gcs)

        # instead of uploading bam, check if bam has been produced to better emulate pipeline
        # basecalling -> alignment in same instance without bam upload 
        successful_bam = len(bam_files) > 0
        print(f"successful_bam={successful_bam} ({len(bam_files)} BAM(s) found)")

        # move all pending_files/ -> processed_files/
        moved = []
        for blob_name in blob_names:
            blob = bucket.blob(blob_name)
            new_name = "processed_files/" + blob_name[len("pending_files/"):]
            bucket.rename_blob(blob, new_name)
            print(f"Moved {blob_name} to {new_name}")
            moved.append(new_name)

        batch_t1 = time.time()

        node_id = ray.get_runtime_context().get_node_id()

        return {
            "inputs": blob_names,
            "inputs_moved_to": moved,
            "output_bams": len(bam_files),
            "successful_bam": successful_bam,
            "num_files": len(blob_names),
            "total_reads": total_reads,
            "total_size_gb": total_size_gb,
            "batch_start": datetime.fromtimestamp(batch_t0).strftime("%Y-%m-%d %H:%M:%S"),
            "batch_end": datetime.fromtimestamp(batch_t1).strftime("%Y-%m-%d %H:%M:%S"),
            "total_batch_seconds": round(batch_t1 - batch_t0, 2),
            "download_seconds": round(t_download_end - t_download_start, 2),
            "basecall_seconds": round(t_dorado_end - t_dorado_start, 2),
            "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES", ""),
            "node_id": node_id,
        }

def parse_ray_status_v():
    """Run `ray status -v` and return (active_worker_nodes, {node_name: idle_ms}, {node_name: gpu_used}).
    Nodes with no Idle line are active (idle_ms=0). Head node is excluded.

    gpu_used is parsed from the Usage section (e.g. '2.0/2.0 GPU'), NOT from the Activity
    section. The Activity section shows stale 'GPU currently in use' even when actual GPU
    allocation is 0 due to the autoscaler v2 WorkFootprint bug (ray-project/ray#60546).
    """
    try:
        result = subprocess.run(["ray", "status", "-v"], capture_output=True, text=True, timeout=10)
        output = result.stdout
    except Exception:
        return 0, {}, {}

    active_nodes = 0
    node_idle = {}
    node_gpu_used = {}
    for block in re.split(r'\nNode: ', output)[1:]:  # skip header section
        lines = block.strip().split('\n')
        header = lines[0]  # eg "ray-han-...-worker-abc (worker_node)"
        if '(head_node)' in header:
            continue
        full_name = header.split(' ')[0]
        m_short = re.search(r'(worker-[a-f0-9]+)', full_name)
        node_name = m_short.group(1) if m_short else full_name
        idle_ms = None
        gpu_used = 0.0
        for line in lines:
            m = re.match(r'\s*Idle:\s*(\d+)\s*ms', line)
            if m:
                idle_ms = int(m.group(1))
            m2 = re.match(r'\s*([\d.]+)/([\d.]+)\s+GPU', line)
            if m2:
                gpu_used = float(m2.group(1))
        if idle_ms is None:
            active_nodes += 1
            idle_ms = 0
        node_idle[node_name] = idle_ms
        node_gpu_used[node_name] = gpu_used

    return active_nodes, node_idle, node_gpu_used


def main():
    ray.init(address="auto")
    bucket_name = "han-ray-urwgs"

    print("Starting bcal_demo…")
    print(f"Bucket={bucket_name}")

    q = Queue(actor_options={"num_cpus": 0, "resources": {"head_slot": 0.001}})

    tracker = ProgressTracker.remote()
    watcher = GCSWatcher.remote(bucket_name, poll_secs=60.0) # check new uploads in GCS every 1 minute
    sched = HeadScheduler.remote(bucket_name, max_in_flight=8) # EDIT HERE IF GPU # CONFIG CHANGES

    print("Starting GCSWatcher...")
    print("Starting HeadScheduler...")

    watcher.run.remote(q)

    gpu_timeline = [] 
    last_gpu_snapshot = 0
    node_cumulative_idle = {}  
    node_idle_since = {}       

    try:
        while True:
            in_flight = ray.get(sched.tick.remote(q, tracker))
            now = time.time()
            if now - last_gpu_snapshot >= 60:
                total_gpus = int(ray.cluster_resources().get("GPU", 0))
                active_nodes, ray_idle_ms, node_gpu_used = parse_ray_status_v()

                # update cumulative idle tracking.
                # use actual GPU usage (node_gpu_used) rather than Ray's idle_ms as the idle signal (ray-project/ray#60546),
                for node_name in ray_idle_ms:
                    is_idle = node_gpu_used.get(node_name, 0.0) == 0.0
                    if node_name not in node_cumulative_idle:
                        node_cumulative_idle[node_name] = 0.0
                        node_idle_since[node_name] = now if is_idle else None
                    
                    if is_idle:
                        if node_idle_since[node_name] is None:
                            node_idle_since[node_name] = now
                        node_cumulative_idle[node_name] += now - node_idle_since[node_name]
                        node_idle_since[node_name] = now
                    else:
                        node_idle_since[node_name] = None
                        node_cumulative_idle[node_name] = 0.0

                cumulative_snapshot = {n: round(s) for n, s in node_cumulative_idle.items()}
                gpu_per_node = {n: int(g) for n, g in node_gpu_used.items()}
                active_gpus = sum(gpu_per_node.values())
                idle_nodes = sum(1 for g in gpu_per_node.values() if g == 0)
                pending_files = len(uploaded_blobs_list(bucket_name, "pending_files/"))
                gpu_timeline.append((ts(), total_gpus, active_gpus, in_flight, pending_files, active_nodes, idle_nodes, ray_idle_ms, cumulative_snapshot, gpu_per_node))

                # TODO REMOVE ONCE FIXED BY RAY
                # manual teardown workaround for autoscaler v2 idle scale-down bug (ray-project/ray#60546).
                hostname_to_node_id = {}
                for n in ray.nodes():
                    if n["Alive"] and not n.get("Resources", {}).get("head_slot"):
                        full_hostname = n["NodeManagerHostname"]
                        m = re.search(r'(worker-[a-f0-9]+)', full_hostname)
                        short_name = m.group(1) if m else full_hostname
                        hostname_to_node_id[short_name] = n["NodeID"]
                for node_name, idle_secs in list(node_cumulative_idle.items()):
                    if idle_secs >= IDLE_TEARDOWN_SECS and gpu_per_node.get(node_name, 0) == 0:
                        node_id = hostname_to_node_id.get(node_name)
                        if node_id:
                            print(f"[{ts()}] Draining idle node {node_name} (cumulative idle={idle_secs:.0f}s >= {IDLE_TEARDOWN_SECS}s)")
                            try:
                                # flags required to trigger graceful termination 
                                subprocess.run(
                                    ["ray", "drain-node", "--node-id", node_id,
                                     "--reason", "DRAIN_NODE_REASON_IDLE_TERMINATION",
                                     "--reason-message", f"idle for {idle_secs:.0f}s (autoscaler v2 workaround)"],
                                    timeout=30, check=True
                                )
                                node_cumulative_idle.pop(node_name, None)
                                node_idle_since.pop(node_name, None)
                                print(f"[{ts()}] Successfully drained {node_name}")
                            except Exception as e:
                                print(f"[{ts()}] Failed to drain {node_name}: {e}")

                last_gpu_snapshot = now
            time.sleep(2) # when percentage tracking will be re-evaluated for task submission
    finally:
        try:
            batches = ray.get(sched.get_completed_batches.remote(), timeout=10)
        except Exception as e:
            print(f"Could not fetch completed batches: {e}")
            batches = []
        if batches:
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=["num_files", "total_reads", "total_size_gb", "batch_start", "batch_end", "total_batch_seconds", "download_seconds", "basecall_seconds", "successful_bam", "node_id", "inputs", "output_bams"])
            writer.writeheader()
            for b in batches:
                writer.writerow({
                    "num_files": b["num_files"],
                    "total_reads": b["total_reads"],
                    "total_size_gb": b["total_size_gb"],
                    "batch_start": b["batch_start"],
                    "batch_end": b["batch_end"],
                    "total_batch_seconds": b["total_batch_seconds"],
                    "download_seconds": b["download_seconds"],
                    "basecall_seconds": b["basecall_seconds"],
                    "successful_bam": b["successful_bam"],
                    "node_id": b.get("node_id", ""),
                    # "inputs": json.dumps(b["inputs"]),
                    "output_bams": b["output_bams"],
                })
            gcs_path = f"run_summary.csv"
            storage.Client().bucket(bucket_name).blob(gcs_path).upload_from_string(buf.getvalue(), content_type="text/csv")
            print(f"Run summary uploaded to gs://{bucket_name}/{gcs_path} ({len(batches)} batches)")

        if gpu_timeline:
            gpu_buf = io.StringIO()
            gpu_writer = csv.DictWriter(gpu_buf, fieldnames=["timestamp", "total_gpus", "active_gpus", "in_flight", "pending_files", "active_nodes", "idle_nodes", "gpu_used_per_node", "ray_idle_ms_per_node", "cumulative_idle_secs_per_node"])
            gpu_writer.writeheader()
            for snapshot_ts, total_gpus, active_gpus, in_flight, pending_files, active_nodes, idle_nodes, ray_idle_ms, cumulative_idle, gpu_per_node in gpu_timeline:
                gpu_writer.writerow({
                    "timestamp": snapshot_ts,
                    "total_gpus": total_gpus,
                    "active_gpus": active_gpus,
                    "in_flight": in_flight,
                    "pending_files": pending_files,
                    "active_nodes": active_nodes,
                    "idle_nodes": idle_nodes,
                    "gpu_used_per_node": json.dumps(gpu_per_node), # actual GPU allocation per node from ray status -v Usage section
                    "ray_idle_ms_per_node": json.dumps(ray_idle_ms), # ray status -v system layer (resets due to ray-project/ray#60546)
                    "cumulative_idle_secs_per_node": json.dumps(cumulative_idle), # application layer tracking (immune to bug)
                })
            storage.Client().bucket(bucket_name).blob("gpu_timeline.csv").upload_from_string(gpu_buf.getvalue(), content_type="text/csv")
            print(f"GPU timeline uploaded to gs://{bucket_name}/gpu_timeline.csv ({len(gpu_timeline)} snapshots)")

if __name__ == "__main__":
    main()
