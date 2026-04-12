#!/bin/bash

if [ $# -ne 3 ]; then
    echo "Usage: simulate_sequencer_gcs_dynamic.sh <runtime_seconds> <src_dir> <gcs_dst>"
    echo "  e.g. bash simulate_sequencer_gcs_dynamic.sh 3600 /mnt/pod5_data gs://han-ray-urwgs/input_files"
    exit 1
fi

RUNTIME=$1
SRC_DIR=$2
GCS_DST=$3

SCRIPT_DIR=$(dirname "$0")

echo "Starting dynamic upload simulation at $(date)"
echo "  Source      : $SRC_DIR"
echo "  Destination : $GCS_DST"
echo "  Runtime     : ${RUNTIME}s across 48 flowcells"
echo "  Upload pace : fast (0.1x) → slow (5x) → regular (1x) per flowcell"

time parallel --line-buffer -j 48 \
    "$SCRIPT_DIR/simulate_flowcell_gcs_dynamic.sh" \
    ::: "$SRC_DIR" \
    ::: "$GCS_DST" \
    ::: {1..6}{A..H} \
    ::: "$RUNTIME"

echo "Simulation complete at $(date)"
