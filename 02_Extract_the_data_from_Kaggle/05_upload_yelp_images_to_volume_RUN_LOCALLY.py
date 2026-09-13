#!/usr/bin/env python3
"""
Upload local Yelp image dataset to a Databricks Unity Catalog Volume.

FULLY AUTOMATIC — just run:
    python3 05_upload_yelp_images_to_volume_RUN_LOCALLY.py

STRATEGY
---------
Uploading 200K+ small files one-by-one via the Files API is bottlenecked
by per-request HTTP overhead (~10-14 hours).  This script uses a much
faster approach:

1. Packs all images into tar archive CHUNKS locally (2 GB each).
   - No compression (JPEGs don't compress), so this is very fast.
2. Uploads just the handful of tar chunks to the UC Volume.
   - ~5 API calls instead of 200,000 — cuts upload from hours to minutes.
3. The tar chunks are then extracted on the Databricks side by running
   notebook 06 ("06_Extract_Tar_Archives_into_Volume").

WHAT THIS SCRIPT DOES
----------------------
1. Auto-creates a virtual environment (.venv) to handle PEP 668
   externally-managed Python installs (Arch, Fedora 38+, Ubuntu 23.04+).
2. Auto-installs missing Python dependencies (databricks-sdk, python-dotenv).
3. Loads DATABRICKS_HOST and DATABRICKS_TOKEN from a .env file.
4. Packs the local Yelp Photos folder into 2 GB tar chunks.
5. Uploads each chunk via the Databricks Files API with retry logic.
6. Tracks which chunks succeeded so re-runs skip already-uploaded chunks.
7. Prints progress with transfer speed and ETA.

AFTER THIS SCRIPT FINISHES
---------------------------
Run notebook 06 ("06_Extract_Tar_Archives_into_Volume") in Databricks.
That notebook will:
- Extract every .tar chunk into the yelp_images_dataset folder
- Delete the tar chunks after extraction to free volume space
- Verify the extracted file count and size

ASSUMPTIONS
------------
1. This script runs on your LOCAL machine, NOT inside Databricks.
2. Python 3.10+ is installed on your local machine.
3. You have internet access to reach PyPI and your Databricks workspace.
4. The Yelp images dataset has been MANUALLY DOWNLOADED from the
   Yelp Open Dataset website (https://www.yelp.com/dataset) onto your
   local machine. This script does NOT download the dataset for you.
5. The downloaded dataset archive has been MANUALLY EXTRACTED on your
   local machine into a folder of image files.
6. The target Unity Catalog volume (yelp_poc_uc.bronze.landing_zone)
   and the yelp_images_dataset folder already exist (created by
   notebooks 01 and 04).
7. A Databricks Personal Access Token (PAT) is in the .env file.

STEPS TO RUN LOCALLY
---------------------
1. Download the Yelp images dataset:
   - Go to https://www.yelp.com/dataset
   - Download the "Photos" dataset archive to your machine
   - Extract the archive to a local folder, e.g.:
         /home/ibrahimhegazi/Downloads/Yelp-Photos/Yelp Photos
     Confirm the extracted folder contains the actual image files
     (not another nested archive).

2. Prepare the .env file:
   - Download the .env file from the Databricks workspace
     (project root: Yelp-AI-Data-Platform-End-to-End-Analytics-NLP-CV-PoC/.env)
   - Place it in the SAME folder as this script on your local machine
   - Verify it contains:
         DATABRICKS_HOST=https://<your-workspace-url>
         DATABRICKS_TOKEN=<your-personal-access-token>

3. Confirm LOCAL_ROOT below matches your actual extracted folder path.

4. Run it:
       python3 05_upload_yelp_images_to_volume_RUN_LOCALLY.py

5. After the script finishes, open notebook 06
   ("06_Extract_Tar_Archives_into_Volume") in Databricks and run the
   "Extract uploaded tar archives" cell to unpack the images into the
   volume and clean up the tar chunks.
"""

# ---------------------------------------------------------------------------
# Step 0: Auto-install missing dependencies
# ---------------------------------------------------------------------------
import os
import subprocess
import sys


VENV_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".venv")


def _ensure_venv():
    """Create a venv and re-launch this script inside it if needed.

    Modern Linux distros (Arch, Fedora 38+, Ubuntu 23.04+) mark the system
    Python as externally-managed (PEP 668), blocking pip installs.  The fix
    is a project-local virtual environment — this function creates one
    automatically and re-execs the script inside it so the user never has
    to think about it.
    """
    # Already inside our venv? Nothing to do.
    if sys.prefix != sys.base_prefix:
        return

    venv_python = os.path.join(VENV_DIR, "bin", "python")

    # Create the venv once (with pip included)
    if not os.path.isfile(venv_python):
        print(f"Creating virtual environment at {VENV_DIR} ...")
        import venv
        venv.create(VENV_DIR, with_pip=True)
        print("Virtual environment created.\n")

    # Re-launch this same script inside the venv
    print(f"Re-launching inside venv: {venv_python}")
    os.execv(venv_python, [venv_python] + sys.argv)


def _ensure_packages():
    """Install any missing project dependencies (runs inside the venv)."""
    required = {"databricks.sdk": "databricks-sdk", "dotenv": "python-dotenv", "tqdm": "tqdm"}
    missing = []
    for import_name, pip_name in required.items():
        try:
            __import__(import_name)
        except ImportError:
            missing.append(pip_name)
    if missing:
        print(f"Installing missing packages: {', '.join(missing)}")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet"] + missing
        )
        print("Done.\n")


_ensure_venv()      # creates venv + re-execs if not already inside it
_ensure_packages()  # installs deps inside the venv

# ---------------------------------------------------------------------------
# Imports (safe now that dependencies are guaranteed)
# ---------------------------------------------------------------------------
import json
import tarfile
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from dotenv import load_dotenv
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
LOCAL_ROOT = "/home/ibrahimhegazi/Downloads/Yelp-Photos/Yelp Photos"
VOLUME_ROOT = "/Volumes/yelp_poc_uc/bronze/landing_zone/yelp_images_dataset"
TAR_DIR = Path(__file__).parent / "_tar_chunks"  # local staging for tar archives
MANIFEST_PATH = Path(__file__).parent / "upload_manifest.json"
CHUNK_SIZE_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB per tar chunk (under 5 GB API limit)
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 10


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def load_env():
    """Load .env from the same directory as this script."""
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=True)
        print(f"Loaded credentials from {env_path}")
    else:
        print(
            f"WARNING: No .env file found at {env_path}\n"
            f"  Create one with DATABRICKS_HOST and DATABRICKS_TOKEN,\n"
            f"  or set them as environment variables before running."
        )

    host = os.environ.get("DATABRICKS_HOST", "")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host or host.startswith("<") or not token or token.startswith("<"):
        sys.exit(
            "ERROR: DATABRICKS_HOST and DATABRICKS_TOKEN must be set.\n"
            "  Edit the .env file next to this script and fill them in."
        )


def load_manifest() -> set:
    if MANIFEST_PATH.exists():
        with open(MANIFEST_PATH) as f:
            return set(json.load(f))
    return set()


def save_manifest(uploaded: set):
    with open(MANIFEST_PATH, "w") as f:
        json.dump(sorted(uploaded), f)


def format_size(bytes_val: float) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if bytes_val < 1024:
            return f"{bytes_val:.1f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.1f} TB"


def format_time(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.0f}m {seconds % 60:.0f}s"
    else:
        h = int(seconds // 3600)
        m = int((seconds % 3600) // 60)
        return f"{h}h {m}m"


def create_tar_chunks(local_root: str, tar_dir: Path, chunk_size: int) -> list:
    """Pack local_root into tar archives, splitting at chunk_size bytes.

    Returns a list of Path objects pointing to the created tar files.
    No compression — JPEGs are already compressed, so tar is just a container.
    """
    tar_dir.mkdir(parents=True, exist_ok=True)

    # Reuse existing tar chunks to avoid re-packing on retry
    existing = sorted(tar_dir.glob("yelp_images_chunk_*.tar"))
    if existing:
        total_size = sum(f.stat().st_size for f in existing)
        print(f"Reusing {len(existing)} existing tar chunk(s) ({format_size(total_size)} total)")
        print(f"  Delete {tar_dir}/ to force re-packing.\n")
        return existing

    chunks = []
    chunk_idx = 0
    current_tar = None
    current_size = 0

    all_files = []
    for dirpath, _, filenames in os.walk(local_root):
        for name in filenames:
            full_path = os.path.join(dirpath, name)
            all_files.append(full_path)

    print(f"Packing {len(all_files)} files into tar chunks ({format_size(chunk_size)} each)...")

    for i, full_path in enumerate(all_files, 1):
        file_size = os.path.getsize(full_path)

        # Start a new chunk if needed
        if current_tar is None or (current_size + file_size > chunk_size and current_size > 0):
            if current_tar is not None:
                current_tar.close()
                print(f"  Chunk {chunk_idx}: {format_size(current_size)}")
            chunk_idx += 1
            chunk_path = tar_dir / f"yelp_images_chunk_{chunk_idx:03d}.tar"
            chunks.append(chunk_path)
            current_tar = tarfile.open(chunk_path, "w")
            current_size = 0

        # Add file to current chunk, preserving relative path
        arcname = os.path.relpath(full_path, local_root)
        current_tar.add(full_path, arcname=arcname)
        current_size += file_size

        if i % 10000 == 0:
            print(f"  Packed {i}/{len(all_files)} files...")

    if current_tar is not None:
        current_tar.close()
        print(f"  Chunk {chunk_idx}: {format_size(current_size)}")

    print(f"\nCreated {len(chunks)} tar chunk(s).\n")
    return chunks


class _ProgressFile:
    """Wraps a binary file to show a tqdm progress bar as bytes are read."""

    def __init__(self, fobj, total: int, desc: str):
        self._fobj = fobj
        self._bar = tqdm(
            total=total, unit="B", unit_scale=True, unit_divisor=1024,
            desc=desc, ncols=80, miniters=1,
        )

    def read(self, size=-1):
        data = self._fobj.read(size)
        if data:
            self._bar.update(len(data))
        return data

    def close(self):
        self._bar.close()
        self._fobj.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def upload_chunk(w: WorkspaceClient, local_path: Path, remote_path: str) -> bool:
    """Upload a single tar chunk with retries and a real-time progress bar."""
    file_size = local_path.stat().st_size
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            start = time.time()
            with _ProgressFile(open(local_path, "rb"), file_size, f"  {local_path.name}") as pf:
                w.files.upload(remote_path, pf, overwrite=True)
            elapsed = time.time() - start
            speed = file_size / elapsed if elapsed > 0 else 0
            print(f"  ✓ {local_path.name} uploaded in {format_time(elapsed)} ({format_size(speed)}/s)")
            return True
        except DatabricksError as e:
            wait = RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1))
            print(f"  retry {attempt}/{MAX_RETRIES} for {local_path.name}: {e} (waiting {wait}s)")
            if attempt < MAX_RETRIES:
                time.sleep(wait)
            else:
                print(f"  FAILED after {MAX_RETRIES} retries: {local_path.name}")
    return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # --- Load credentials from .env automatically ---
    load_env()

    # --- Validate local folder ---
    if not os.path.isdir(LOCAL_ROOT):
        sys.exit(f"Local folder not found: {LOCAL_ROOT}")

    # --- Connect to Databricks ---
    w = WorkspaceClient()
    print(f"Connected to: {os.environ.get('DATABRICKS_HOST')}\n")

    # --- Step 1: Create tar chunks locally ---
    tar_chunks = create_tar_chunks(LOCAL_ROOT, TAR_DIR, CHUNK_SIZE_BYTES)
    if not tar_chunks:
        sys.exit("No files found to upload.")

    # --- Step 2: Upload each chunk to the volume ---
    uploaded = load_manifest()
    failed = []
    start_time = time.time()
    total_bytes = 0

    print(f"Uploading {len(tar_chunks)} tar chunk(s) to {VOLUME_ROOT}/...\n")

    for chunk_path in tar_chunks:
        chunk_name = chunk_path.name
        if chunk_name in uploaded:
            print(f"  Skipping {chunk_name} (already uploaded)")
            continue

        remote_path = f"{VOLUME_ROOT}/{chunk_name}"
        if upload_chunk(w, chunk_path, remote_path):
            uploaded.add(chunk_name)
            total_bytes += chunk_path.stat().st_size
            save_manifest(uploaded)
        else:
            failed.append(chunk_name)

    elapsed_total = time.time() - start_time

    # --- Step 3: Clean up local tar chunks (only successfully uploaded) ---
    print("\nCleaning up local tar chunks...")
    for chunk_path in tar_chunks:
        if chunk_path.exists() and chunk_path.name in uploaded:
            chunk_path.unlink()
    # Only remove the staging directory if it is empty
    if TAR_DIR.exists() and not any(TAR_DIR.iterdir()):
        TAR_DIR.rmdir()
    if failed:
        print(f"Kept {len(failed)} failed chunk(s) locally for retry.")
    else:
        print("All local tar chunks removed.")
        if MANIFEST_PATH.exists():
            MANIFEST_PATH.unlink()

    # --- Summary ---
    print(f"\n{'=' * 60}")
    print(f"UPLOAD COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Chunks uploaded:  {len(uploaded)}/{len(tar_chunks)}")
    print(f"  Data transferred: {format_size(total_bytes)}")
    print(f"  Time elapsed:     {format_time(elapsed_total)}")
    if total_bytes > 0 and elapsed_total > 0:
        print(f"  Avg speed:        {format_size(total_bytes / elapsed_total)}/s")
    if failed:
        print(f"\n  {len(failed)} chunk(s) FAILED:")
        for f in failed:
            print(f"    - {f}")
        print("  Re-run this script to retry failed chunks.")
    else:
        print(f"\n  All chunks uploaded successfully.")
        print(f"  NEXT STEP: Run notebook 06 (06_Extract_Tar_Archives_into_Volume)")
        print(f"  in Databricks to unpack images into the volume.")


if __name__ == "__main__":
    main()
