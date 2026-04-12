#!/bin/bash

SRC_DIR=$1        # local root, e.g. /mnt/pod5_data
GCS_DST=$2        # GCS prefix, e.g. gs://han-ray-urwgs/input_files
FLOWCELL=$3       # e.g. 1A, 2B, ...
RUNTIME=$4        # total simulation duration in seconds

# speed multipliers relative to base period
FAST_FACTOR=0.1
SLOW_FACTOR=3.0

FLOWCELL_DIR=$(find "$SRC_DIR/no_sample" -maxdepth 1 -type d -name "*_${FLOWCELL}" 2>/dev/null | head -1)

if [ -z "$FLOWCELL_DIR" ]; then
    echo "[${FLOWCELL}] No directory found, skipping."
    exit 0
fi

mapfile -t FILES < <(find "$FLOWCELL_DIR" -name '*.pod5' | sort)
NUM_POD5=${#FILES[@]}

if [ "$NUM_POD5" -eq 0 ]; then
    echo "[${FLOWCELL}] No pod5 files found, skipping."
    exit 0
fi

THIRD=$((NUM_POD5 / 3))

read -r BASE_PERIOD FAST_PERIOD SLOW_PERIOD < <(python3 -c "
base = abs(${RUNTIME} / ${NUM_POD5})
fast = max(base * ${FAST_FACTOR}, 0.5)
slow = base * ${SLOW_FACTOR}
print(f'{base:.2f} {fast:.2f} {slow:.2f}')
")

echo "[${FLOWCELL}] $NUM_POD5 files, thirds=${THIRD} — fast=${FAST_PERIOD}s / slow=${SLOW_PERIOD}s / regular=${BASE_PERIOD}s"

for i in "${!FILES[@]}"; do
    if   [ "$i" -lt "$THIRD" ];           then PERIOD=$FAST_PERIOD;   LABEL="fast"
    elif [ "$i" -lt "$((THIRD * 2))" ];   then PERIOD=$SLOW_PERIOD;   LABEL="slow"
    else                                       PERIOD=$BASE_PERIOD;    LABEL="regular"
    fi

    filename=$(basename "${FILES[$i]}")
    gsutil cp "${FILES[$i]}" "${GCS_DST}/${filename}"
    echo "[${FLOWCELL}] [$LABEL] Uploaded: ${filename} (sleep ${PERIOD}s)"
    sleep "${PERIOD}s"
done
