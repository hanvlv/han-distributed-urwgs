#!/bin/bash

SRC_DIR=$1        # local root, e.g. /mnt/pod5_data
GCS_DST=$2        # GCS prefix, e.g. gs://han-ray-urwgs/input_files
FLOWCELL=$3       # e.g. 1A, 2B, ...
RUNTIME=$4        # total simulation duration in seconds

FLOWCELL_DIR=$(find "$SRC_DIR/no_sample" -maxdepth 1 -type d -name "*_${FLOWCELL}" 2>/dev/null | head -1)

if [ -z "$FLOWCELL_DIR" ]; then
    echo "[${FLOWCELL}] No directory found, skipping."
    exit 0
fi

NUM_POD5=$(find "$FLOWCELL_DIR" -name '*.pod5' | wc -l)

if [ "$NUM_POD5" -eq 0 ]; then
    echo "[${FLOWCELL}] No pod5 files found, skipping."
    exit 0
fi

PERIOD=$(python3 -c "print(abs(int(${RUNTIME}/${NUM_POD5})))")
echo "[${FLOWCELL}] $NUM_POD5 files — uploading 1 every ${PERIOD}s"

# flowcell agnostic upload results 
while IFS= read -r pod5_file; do
    filename=$(basename "$pod5_file")
    gsutil cp "$pod5_file" "${GCS_DST}/${filename}"
    echo "[${FLOWCELL}] Uploaded: ${filename}"
    sleep "${PERIOD}s"
done < <(find "$FLOWCELL_DIR" -name '*.pod5')
