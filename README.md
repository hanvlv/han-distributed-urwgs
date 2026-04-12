# Adaptive Distributed Architecture for Ultra-Rapid Whole-Genome Sequencing

*Adaptive Distributed Architecture for Ultra-Rapid Whole-Genome Sequencing* by Jung Han Lee, advised by Prof. Sneha D. Goenka (April 2026).

This repository implements a flowcell-agnostic, Ray-based distributed genomic analysis pipeline for ultra-rapid whole-genome sequencing (urWGS) on Google Cloud Platform. The system replaces static flowcell-to-VM mappings with dynamic, resource-aware task scheduling and automated autoscaling to enhance resource utilizaiton and efficiency while reducing operational cost.

---

## Overview

Oxford Nanopore's PromethION 48 sequencer produces ~2 TB of POD5 signal data across 48 flowcells within 90 minutes. The previous urWGS pipeline statically pre-assigned flowcell groups to cloud VM instances at startup. Since nanopore sensor yield is inherently variable, this produced load imbalance and tail latency: most VMs finish early while one bottleneck instance keeps processing.

This work replaces that design with a **flowcell-agnostic** Ray cluster that:
- Pools all incoming POD5 files from cloud storage into a single task queue
- Dispatches fixed-size batches (10 files, ~9.3 GB each) to GPU workers via bin-packing + recency-aware scheduling
- Autoscales worker nodes up and down in response to live queue depth
- Tracks per-task basecalling progress to avoid triggering unnecessary node launches when a GPU is nearly free


## System Configuration 

| Node | Machine Type | vCPUs | Memory | GPU | Disk | Role |
|---|---|---|---|---|---|---|
| Head Node | `e2-standard-4` | 4 | 16 GB | — | Standard persistent disk | Runs `GCSWatcher`, `HeadScheduler`, `ProgressTracker`; dispatches batches to workers |
| Worker Node | `n1-custom-24-92160` | 24 | 90 GB | 4× NVIDIA V100 | Local NVMe SSD | Runs `BasecallingOperator`; downloads POD5, basecalls with Dorado, uploads BAM |

<p align="center">
  <img src="https://github.com/user-attachments/assets/6397f4d1-d6d9-4d78-b233-fcb6cfae883f" alt="config_image" width="600">
</p>

The node types, GPU counts, and instance sizes shown below reflect the configuration used for this simulation and evaluation. These configurations are not permanent and can be changed accordingly for other hardware or deployment targets.

## Pipeline Architecture

```
PromethION 48 Sequencer
        │  upload sequenced POD5 files
        ▼
  Google Cloud Storage (input_files/)
        │  poll every 60s
        ▼
  Head Node (e2-standard-4)
  ├── GCSWatcher      — polls GCS, renames blobs input→pending, enqueues paths
  ├── HeadScheduler   — bin-packing dispatcher with progress-aware threshold guard
  └── ProgressTracker — tracks per-task read completion %
        │  dispatch batches (NodeAffinitySchedulingStrategy)
        ▼
  Worker Nodes (n1-custom-24-92160, 2× NVIDIA V100, local NVMe SSD)
  └── BasecallingOperator — downloads POD5 → runs Dorado → uploads BAM
```

<p align="center">
  <img src="https://github.com/user-attachments/assets/c2df33aa-b071-4b1f-870d-ae1467a897d4" alt="pipeline_image" width="600">
</p>

## Scheduling Logic 

The pipeline implements the following scheduling mechanisms for inter-node task allocation:

| Mechanism | Behavior |
|---|---|
| Bin-packing | Tasks are consolidated onto as few nodes as possible (Ray PACK strategy), keeping idle nodes free to accumulate idle time and be terminated by the autoscaler |
| Recency-aware tie-breaking | When multiple nodes have equal free capacity, the scheduler prefers the most recently active node, preserving accumulated idle time on longer-idle nodes and allowing the autoscaler to terminate them sooner |
| Backpressure control | A configurable cap on concurrent in-flight task submissions prevents flooding Ray's object store before GPU capacity is available |
| Progress-threshold scheduling | The head scheduler tracks live basecalling read completion (via Dorado's sequencing summary file); when all GPUs are occupied but a task exceeds 90% completion, the next batch is submitted as a pending future against that GPU slot instead of triggering a new node launch |


## Key Design Decisions

Adjustable variables in `bcal_demo.py`--these variables can also be changed accordingly depending on pipeline implementations and hardware utilizations. 

| Component | Variable | Default | Reason |
|---|---|---|---|
| Batch size | `BATCH_SIZE` | `10` POD5 files (~9.3 GB) | Amortizes Dorado model-load overhead (~90s) across sufficient work |
| Batch timeout | `BATCH_TIMEOUT_SECS` | `300` sec | Dispatches a partial batch if it hasn't filled within this window |
| Progress threshold | `BC_PROG_THRESH` | `90.0`% read completion | Avoids launching a new node when an existing GPU slot is about to free |
| Idle timeout | `IDLE_TEARDOWN_SECS` | `600` sec (10 min) | Balances re-provisioning cost vs. instance retention cost; must match `idle_timeout_minutes` in the YAML |
| Dorado batch size | (hardcoded in YAML config) | `5120` | Pre-benchmarked on V100 16 GB VRAM + `fast@v5.0.0`; skips per-task auto-benchmark |
| Local NVMe SSD | (YAML config) | Attached at instance creation | Eliminates network-attached disk bottleneck, significantly accelerating POD5 downloads |
| Custom VM images | (YAML config) | Pre-baked CUDA, Dorado, Python | Reduces worker bring-up from 8–10 min (fresh install) to 2–4 min |

### Autoscaler Idle-Timeout Bug

While implementing Ray's autoscaler, I encountered a bug where worker nodes failed to scale down even after their idle timeout was reached. Each node's idle timer, internal to Ray and not exposed to users, is supposed to track only that node's own inactivity and remain independent from peer node activities. However, Ray's bug defaulted to all node timers resetting on any cluster-wide activity, even when the node itself was idle, resulting in continuous timer resets that indefinitely prevented scale-down.

Since a proper fix required changes to Ray's internals, I temporarily worked around it by tracking per-node idle time independently in this pipeline and manually triggering teardown once a node's idle timeout was reached.

The github ticket can be tracked [here](https://github.com/ray-project/ray/issues/62430).

## Requirements

### Local machine (cluster management)
- [Ray](https://docs.ray.io) (`pip install ray[default]`)
- Google Cloud SDK (`gcloud`)
- SSH key configured for GCP (`~/.ssh/google_compute_engine`)

### GCP project
- GCS bucket for POD5 input and BAM output
- Service accounts with `Service Account User`, `Compute Admin`, `Storage Admin` IAM roles
- Sufficient V100 GPU quota in target region supported in lab setting (Goenka Lab)

### Worker node image
Each worker VM must have pre-installed:
- CUDA + NVIDIA drivers
- [Dorado](https://github.com/nanoporetech/dorado) basecaller binary + models
- Python 3, `ray`, `google-cloud-storage`, `pod5`

## Pipeline Quickstart 

### 1. Configure cluster environment

Edit `bcal_startup_v5.yaml`:
- Set `provider.project_id` to GCP project
- Set `auth.ssh_user` to GCP SSH username
- Update `sourceImage` fields to custom VM image

Images can be created from an existing VM with the following command:
```bash
gcloud compute images create <image_name> \      
  --source-disk=<VM_name>   \
  --source-disk-zone=<region> \
  --project=ece-goenkalab

```
- Change `max_workers` and `resources` to adjust GPU count

### 2. Launch the Ray cluster

```bash
ray up -y basecaller_ray/bcal_startup_v5.yaml
```

SSH into the head node when ready:

```bash
ray attach basecaller_ray/bcal_startup_v5.yaml
```

### 3. Simulate sequencing uploads

From the sequencer VM (or any machine with GCS access), upload POD5 files to simulate a live sequencing run.

To split existing large POD5 files into ~1 GB chunks first:
```bash
SRC_BUCKET=<src> DST_BUCKET=<dst> python sequencer_simulation/split_pod5_gcs.py
```

**Uniform uploads** (steady rate across all 48 flowcells):
```bash
bash sequencer_simulation/simulate_sequencer_gcs.sh \
    <runtime_seconds> <local_pod5_dir> gs://<bucket>/input_files
```

**Dynamic uploads** (fast → slow → regular phases):
```bash
bash sequencer_simulation/simulate_sequencer_gcs_dynamic.sh \
    <runtime_seconds> <local_pod5_dir> gs://<bucket>/input_files
```

### 4. Run the pipeline

From the head node:

```bash
python basecaller_ray/bcal_demo.py 
```

Key environment variables:
```bash
DORADO_BIN=~/dorado-1.4.0-linux-x64/bin/dorado
DORADO_MODELS_DIR=~/dorado_models
```

### 5. Monitor

From the head node:
```bash
 ray monitor basecaller_ray/bcal_startup_v5.yaml
```

Access the Ray Dashboard via SSH tunnel:
```bash
ssh -L 8265:localhost:8265 <head-node-ip>
# then open http://localhost:8265
```

Dashboard provides real-time resource monitoring:
<p align="center">
  <img src="https://github.com/user-attachments/assets/307e942d-a26e-4f1f-baf7-4de7c0c51787" alt="dashboard_image">
</p>

### 6. Tear down

```bash
ray down -y basecaller_ray/bcal_startup_v5.yaml
```

## Evaluation

Simulations used 2,194 GB of anonymized patient POD5 data across 48 flowcell directories (2,487 files total) from an ultra-rapid diagnostics study at Stanford Hospitals. Both uniform and dynamic upload scenarios produced consistent per-batch workloads (~9.97 files, ~73,900 reads, ~9.33 GB) across 253 total batches with dynamic node adjustments depending on sequencing demand, confirming efficient sequencing-agnostic scheduling behavior.

<p align="center">
  <img src="https://github.com/user-attachments/assets/ebce8e50-7cc7-4b92-8e68-06ecf7c510db" alt="evaluation_image" width="800">
</p>

See Chapter 5 of the thesis for full results, including per-node GPU idle time heatmaps and autoscaler node-count traces.

## Future Work

- **Higher-accuracy Dorado models.** Benchmarking was conducted exclusively with `fast@v5.0.0`, which prioritizes throughput over accuracy and is not representative of clinical-grade basecalling. Switching to `hac` (high-accuracy) or `sup` (super-accuracy) models would substantially improve basecalling accuracy, at the cost of higher per-batch compute time and VRAM usage — both would require re-running the Dorado batch-size benchmark (currently hardcoded to `5120` for `fast` on 16 GB V100s) to find the optimal batch size for the new model.
- **Scaling to more nodes/GPUs.** The current cluster caps at 4 worker nodes (8 V100 GPUs total) due to lab quota limits. The full-scale production urWGS pipeline this work is intended to integrate with operates across 64 V100 GPUs — an eight-fold increase in capacity over this proof of concept. At that scale, it's worth re-validating that the bin-packing/recency-aware scheduler and idle-timeout autoscaling behavior (tuned and evaluated at 4 nodes) continue to hold up, since contention patterns and idle-time distributions change as node count grows.
- **Merging basecalling and alignment into a single Ray task.** The current implementation terminates each Ray task after BAM generation; alignment is not yet part of the distributed pipeline. Folding alignment into the same task as basecalling would let GPU-accelerated basecalling and CPU-bound alignment run concurrently on the same instance (mirroring the legacy urWGS pipeline's in-memory BAM handoff), giving the Ray task end-to-end ownership of both compute stages and eliminating the inter-stage coordination overhead of treating them as separate scheduled units.

## Useful Links
- Oxford Nanopore POD5 data, resources, and documentation can be found here: [ONT POD5 data](https://epi2me.nanoporetech.com/tutorials/) 
- Google doc for Ray documentation: [Ray V2 Architecture (2022)](https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview?tab=t.0#heading=h.iyrm5j2gcdoq)

## Citation

```
J. H. Lee, "Adaptive Distributed Architecture for Ultra-Rapid Whole-Genome Sequencing,"
Senior Thesis, Department of Electrical and Computer Engineering,
Princeton University, April 2026.
```
