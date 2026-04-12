"""
split_pod5_gcs.py

Read pod5 files from a source GCS bucket, split them into ~1 GB chunks,
and upload each chunk to a destination GCS bucket.

Requirements:
    pip install pod5 google-cloud-storage

Notes:
    - Must run on a machine with enough local disk for the largest single source
      file (e.g. a GCP VM with a large attached disk for 200 GB files).
    - CHUNK_BYTES controls target chunk size (default 1 GiB). Actual sizes may
      vary ±10% because splitting is done by estimated read count, not bytes.
    - pod5 >= 0.3.0 is required.
"""

import itertools
import logging
import os
import tempfile
import uuid
from pathlib import Path
import pod5
from google.cloud import storage

SRC_BUCKET  = os.environ.get("SRC_BUCKET",  "urwgs_prom_data")
DST_BUCKET  = os.environ.get("DST_BUCKET",  "han-patient-data")
SRC_PREFIX  = os.environ.get("SRC_PREFIX",  "swift_dev/prom/hg002-ne-v3-sup6")
DST_PREFIX  = os.environ.get("DST_PREFIX",  "pod5_1gb")  # destination folder
CHUNK_BYTES = int(os.environ.get("CHUNK_BYTES", 1 * 1024 ** 3))  # 1 GiB

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger(__name__)


def list_pod5_blobs(client: storage.Client, bucket_name: str, prefix: str):
    """Yield all .pod5 blobs under gs://bucket_name/prefix."""
    p = (prefix.strip("/") + "/") if prefix else ""
    for blob in client.list_blobs(bucket_name, prefix=p or None):
        if blob.name.lower().endswith(".pod5"):
            yield blob


def upload_blob(dst_bucket: storage.Bucket, local_path: Path, blob_name: str) -> None:
    blob = dst_bucket.blob(blob_name)
    blob.upload_from_filename(str(local_path))
    log.info("    -> uploaded gs://%s/%s (%.1f MB)",
             dst_bucket.name, blob_name, local_path.stat().st_size / 1e6)


def split_and_upload(
    client:          storage.Client,
    src_blob:        storage.Blob,
    src_prefix:      str,
    dst_bucket_name: str,
    dst_prefix:      str,
    chunk_bytes:     int,
    tmpdir:          Path,
) -> None:
    blob_path  = Path(src_blob.name)
    stem       = blob_path.stem

    # preserve sub-folder structure relative to src_prefix.
    # e.g. src_prefix="raw/" blob="raw/runA/foo.pod5" -> rel_folder="runA/"
    sp = (src_prefix.strip("/") + "/") if src_prefix else ""
    blob_rel   = src_blob.name[len(sp):]          
    rel_folder = str(Path(blob_rel).parent)        
    rel_folder = "" if rel_folder == "." else rel_folder.rstrip("/") + "/"

    src_size_b = src_blob.size

    # dpwnload source blob to local temp file 
    local_src = tmpdir / f"{uuid.uuid4().hex}_src.pod5"
    log.info(
        "Downloading gs://%s/%s  (%.2f GB)...",
        src_blob.bucket.name, src_blob.name, src_size_b / 1e9,
    )
    src_blob.download_to_filename(str(local_src))
    actual_bytes = local_src.stat().st_size

    # estimate reads per 1 GB chunk 
    with pod5.Reader(local_src) as reader:
        total_reads = reader.num_reads

    if total_reads == 0:
        log.warning("  Skipping empty file: %s", src_blob.name)
        local_src.unlink(missing_ok=True)
        return

    # use 90% of the target to give a safety margin on size estimation
    reads_per_chunk = max(1, int((chunk_bytes / actual_bytes) * total_reads * 0.9))
    num_chunks = -(-total_reads // reads_per_chunk)  # ceiling division
    log.info(
        "  %d reads | estimated %d reads/chunk | %d chunks",
        total_reads, reads_per_chunk, num_chunks,
    )

    # stream reads in batches and write each batch to a chunk file 
    dst_bucket = client.bucket(dst_bucket_name)
    dp = (dst_prefix.strip("/") + "/") if dst_prefix else ""

    with pod5.Reader(local_src) as reader:
        reads_iter = reader.reads()
        chunk_idx  = 0

        while True:
            # pull the next batch of reads into memory
            batch = list(itertools.islice(reads_iter, reads_per_chunk))
            if not batch:
                break

            local_dst = tmpdir / f"{uuid.uuid4().hex}_chunk.pod5"
            with pod5.Writer(local_dst) as writer:
                for read_record in batch:
                    # convert ReadRecord -> Read (writable representation)
                    writer.add_read(read_record.to_read())

            blob_name = f"{dp}{rel_folder}{stem}_chunk{chunk_idx:04d}.pod5"
            log.info(
                "  [%d/%d] %d reads | %.1f MB",
                chunk_idx + 1, num_chunks,
                len(batch), local_dst.stat().st_size / 1e6,
            )
            upload_blob(dst_bucket, local_dst, blob_name)
            local_dst.unlink(missing_ok=True)
            chunk_idx += 1

    local_src.unlink(missing_ok=True)
    log.info("  Done: %d chunk(s) written for %s", chunk_idx, src_blob.name)


def main() -> None:
    client = storage.Client()

    blobs = list(list_pod5_blobs(client, SRC_BUCKET, SRC_PREFIX))
    log.info(
        "Found %d pod5 file(s) in gs://%s/%s",
        len(blobs), SRC_BUCKET, SRC_PREFIX,
    )

    if not blobs:
        log.warning("Nothing to do.")
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        for i, blob in enumerate(blobs, 1):
            log.info("── File %d/%d: %s ──", i, len(blobs), blob.name)
            split_and_upload(
                client=client,
                src_blob=blob,
                src_prefix=SRC_PREFIX,
                dst_bucket_name=DST_BUCKET,
                dst_prefix=DST_PREFIX,
                chunk_bytes=CHUNK_BYTES,
                tmpdir=tmp,
            )

    log.info("All done.")


if __name__ == "__main__":
    main()
