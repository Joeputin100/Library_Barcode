#!/usr/bin/env bash
# pip_requirements_install.sh — pip install with optional swap & notifications

set -euo pipefail

#————— Configuration —————
REQ_FILE="${1:-requirements-termux.txt}"
LOG_FILE="${LOG_FILE:-$HOME/pip-install.log}"
SWAP_MB="${SWAP_MB:-512}"
SWAP_FILE="${SWAP_FILE:-$HOME/.termux_swap}"
TITLE="Requirements Install"

#————— Sanity check —————
if [[ ! -f "$REQ_FILE" ]]; then
  echo "Error: '$REQ_FILE' not found." >&2
  exit 1
fi

#————— Wake lock —————
command -v termux-wake-lock &>/dev/null && termux-wake-lock

#————— Attempt swap setup —————
if command -v mkswap &>/dev/null && command -v swapon &>/dev/null; then
  echo "[$(date +'%T')] Creating ${SWAP_MB}M swap at $SWAP_FILE" >>"$LOG_FILE"
  [[ -f "$SWAP_FILE" ]] || {
    fallocate -l "${SWAP_MB}M" "$SWAP_FILE" \
      || dd if=/dev/zero of="$SWAP_FILE" bs=1M count="$SWAP_MB"
    mkswap "$SWAP_FILE" >>"$LOG_FILE" 2>&1
  }
  swapon "$SWAP_FILE" >>"$LOG_FILE" 2>&1 || \
    echo "[$(date +'%T')] Warning: swapon failed (need root?)" >>"$LOG_FILE"
else
  echo "[$(date +'%T')] Warning: swap tools missing; skipping swap" >>"$LOG_FILE"
fi

#————— Throttle builds —————
export MAKEFLAGS="-j1"
export OMP_NUM_THREADS="1"

#————— Install loop —————
echo "[$(date +'%T')] Starting pip installs…" >>"$LOG_FILE"
FAILURE=""
while IFS= read -r pkg || [[ -n "$pkg" ]]; do
  [[ -z "$pkg" || "${pkg:0:1}" == "#" ]] && continue
  echo "[$(date +'%T')] Installing $pkg" >>"$LOG_FILE"
      nice -n 19 pip install --no-cache-dir $pkg >>"$LOG_FILE" 2>&1 \
    || { FAILURE="$pkg"; break; }
done < "$REQ_FILE"

#————— Teardown swap —————
if [[ -f "$SWAP_FILE" ]]; then
  command -v swapoff &>/dev/null && swapoff "$SWAP_FILE" >>"$LOG_FILE" 2>&1 \
    || echo "[$(date +'%T')] Warning: swapoff failed" >>"$LOG_FILE"
fi

#————— Notification —————
if command -v termux-notification &>/dev/null; then
  if [[ -z "$FAILURE" ]]; then
    termux-notification \
      --title "$TITLE Success" \
      --content "All packages installed." \
      --priority low
    EXIT_CODE=0
  else
    termux-notification \
      --title "$TITLE Failed" \
      --content "Error on '$FAILURE'. Log: $LOG_FILE" \
      --priority high
    EXIT_CODE=1
  fi
fi

#————— Release wake lock —————
command -v termux-wake-unlock &>/dev/null && termux-wake-unlock

exit "${EXIT_CODE:-0}"
