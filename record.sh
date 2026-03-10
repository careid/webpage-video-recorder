#!/usr/bin/env bash

# Wrapper script for webpage-video-recorder
# Automatically uses Docker on macOS and WSL (where Xvfb/PulseAudio aren't available)
# On native Linux, runs directly with Node.js

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
IMAGE_NAME="webpage-recorder"

# Detect if running inside WSL
is_wsl() {
  grep -qEi "(microsoft|wsl)" /proc/version 2>/dev/null
}

# Detect platform — use Docker on macOS or WSL
needs_docker() {
  [[ "$(uname -s)" != "Linux" ]] || is_wsl
}

# Build Docker image if it doesn't exist or Dockerfile is newer
ensure_image() {
  local image_exists
  image_exists=$(docker images -q "$IMAGE_NAME" 2>/dev/null || true)

  if [[ -z "$image_exists" ]]; then
    echo "[record.sh] Docker image '$IMAGE_NAME' not found. Building..."
    docker build -t "$IMAGE_NAME" "$SCRIPT_DIR"
  else
    # Rebuild if Dockerfile is newer than the image
    local dockerfile_mod
    dockerfile_mod=$(stat -f %m "$SCRIPT_DIR/Dockerfile" 2>/dev/null || stat -c %Y "$SCRIPT_DIR/Dockerfile" 2>/dev/null)
    local image_created
    image_created=$(docker inspect -f '{{.Created}}' "$IMAGE_NAME" 2>/dev/null || echo "")

    if [[ -z "$image_created" ]]; then
      echo "[record.sh] Rebuilding Docker image..."
      docker build -t "$IMAGE_NAME" "$SCRIPT_DIR"
    fi
  fi
}

# Rewrite host paths to container paths for Docker
# Converts ./recordings/... to /app/recordings/...
rewrite_args() {
  local args=()
  local recordings_dir="$SCRIPT_DIR/recordings"

  for arg in "$@"; do
    # Rewrite absolute paths under recordings/ to /app/recordings/
    if [[ "$arg" == "$recordings_dir"* ]]; then
      arg="/app/recordings${arg#$recordings_dir}"
    # Rewrite relative paths like ./recordings/ or recordings/
    elif [[ "$arg" == ./recordings/* ]]; then
      arg="/app/recordings/${arg#./recordings/}"
    elif [[ "$arg" == recordings/* ]]; then
      arg="/app/recordings/${arg#recordings/}"
    fi
    args+=("$arg")
  done

  echo "${args[@]}"
}

if needs_docker; then
  if is_wsl; then
    echo "[record.sh] WSL detected — running via Docker"
  else
    echo "[record.sh] macOS detected — running via Docker"
  fi
  ensure_image

  # Ensure recordings directory exists on host
  mkdir -p "$SCRIPT_DIR/recordings"

  # Rewrite paths for container
  rewritten_args=$(rewrite_args "$@")

  # Run in Docker with recordings volume mounted
  docker run --rm \
    -v "$SCRIPT_DIR/recordings:/app/recordings" \
    "$IMAGE_NAME" \
    $rewritten_args
else
  # Linux — run directly
  exec node "$SCRIPT_DIR/record.js" "$@"
fi
