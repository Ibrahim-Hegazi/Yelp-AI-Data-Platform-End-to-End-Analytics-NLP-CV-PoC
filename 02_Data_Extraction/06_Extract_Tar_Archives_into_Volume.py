# Databricks notebook source
# DBTITLE 1,Notebook Title
# MAGIC %md
# MAGIC # Extract Tar Archives into Volume
# MAGIC
# MAGIC Unpacks the `.tar` chunks uploaded by the local script (`05_upload_yelp_images_to_volume_RUN_LOCALLY.py`) into the `yelp_images_dataset` folder in the Bronze landing zone.

# COMMAND ----------

# DBTITLE 1,Assumptions & Prerequisites
# MAGIC %md
# MAGIC ## Assumptions
# MAGIC
# MAGIC 1. The local upload script (`05_upload_yelp_images_to_volume_RUN_LOCALLY.py`) has **already finished** uploading all `.tar` chunks to `/Volumes/yelp_poc_uc/bronze/landing_zone/yelp_images_dataset/`.
# MAGIC 2. The Yelp images dataset was **manually downloaded** from the [Yelp Open Dataset](https://www.yelp.com/dataset) website onto a local machine.
# MAGIC 3. The downloaded archive was **manually extracted** locally before the upload script packed and uploaded it as tar chunks.
# MAGIC 4. The Unity Catalog volume `yelp_poc_uc.bronze.landing_zone` and the `yelp_images_dataset` folder inside it already exist (created by notebooks 01 and 04).
# MAGIC 5. This notebook runs on **Databricks serverless compute** (no special cluster required).
# MAGIC
# MAGIC ## Steps
# MAGIC
# MAGIC 1. **Run Cell 3** ("Extract uploaded tar archives") — finds all `.tar` chunks in the volume, extracts each one in place, and deletes the tar file after successful extraction.
# MAGIC 2. **Run Cell 4** ("Verify extracted files") — counts and lists the extracted image files to confirm everything landed correctly.
# MAGIC 3. Once verified, the images are ready for downstream pipeline steps (Bronze → Silver → Gold).
# MAGIC
# MAGIC > **Idempotent**: If no `.tar` files are found, the extraction cell simply reports "nothing to extract" — safe to re-run at any time.

# COMMAND ----------

# DBTITLE 1,Extract uploaded tar archives
import os
import tarfile

# --------------------------------------------------
# Extract tar chunks uploaded by the local script
# (05_upload_yelp_images_to_volume_RUN_LOCALLY.py) into the volume.
#
# This cell:
# 1. Finds all .tar chunks in the yelp_images_dataset folder
# 2. Extracts each one into the same folder (preserving paths)
# 3. Deletes the .tar chunk after successful extraction
#
# Idempotent: safe to re-run — skips if no .tar files found.
# --------------------------------------------------

volume_path = "/Volumes/yelp_poc_uc/bronze/landing_zone/yelp_images_dataset"

# Find all tar chunks
tar_files = sorted(f for f in os.listdir(volume_path) if f.endswith(".tar"))

if not tar_files:
    print("No .tar chunks found — nothing to extract.")
    print("Either the images are already extracted, or the upload script hasn't run yet.")
else:
    print(f"Found {len(tar_files)} tar chunk(s) to extract:\n")
    total_extracted = 0

    for tar_name in tar_files:
        tar_path = os.path.join(volume_path, tar_name)
        print(f"  Extracting {tar_name}...")

        with tarfile.open(tar_path, "r") as tf:
            members = tf.getmembers()
            tf.extractall(path=volume_path)
            total_extracted += len(members)
            print(f"    -> {len(members)} files extracted")

        # Remove the tar chunk to free volume space
        os.remove(tar_path)
        print(f"    -> {tar_name} deleted")

    print(f"\nDone. {total_extracted} files extracted into {volume_path}")
    print(f"Tar chunks cleaned up — only image files remain.")

# COMMAND ----------

# DBTITLE 1,Verify extracted files
import os

# --------------------------------------------------
# Verify the extracted images landed correctly
# --------------------------------------------------

volume_path = "/Volumes/yelp_poc_uc/bronze/landing_zone/yelp_images_dataset"

file_count = 0
total_size = 0
extensions = {}

for dirpath, _, filenames in os.walk(volume_path):
    for name in filenames:
        full_path = os.path.join(dirpath, name)
        file_count += 1
        total_size += os.path.getsize(full_path)
        ext = os.path.splitext(name)[1].lower()
        extensions[ext] = extensions.get(ext, 0) + 1

size_gb = total_size / (1024 ** 3)

print(f"Total files:  {file_count:,}")
print(f"Total size:   {size_gb:.2f} GB")
print(f"\nFile types:")
for ext, count in sorted(extensions.items(), key=lambda x: -x[1]):
    print(f"  {ext or '(no ext)':12s} {count:>8,}")

# COMMAND ----------

# DBTITLE 1,Cleanup note
# MAGIC %md
# MAGIC > **After verification**: If the file count and size match your expectations (∼200K images, ∼8.5 GB), the images are ready for the Bronze → Silver → Gold pipeline. The `.tar` chunks have already been deleted by the extraction cell above, so no additional cleanup is needed.
# MAGIC >
# MAGIC > If the count is lower than expected, re-run the local upload script (`05_upload_yelp_images_to_volume_RUN_LOCALLY.py`) — it will resume from the manifest and upload only missing chunks. Then re-run this notebook.