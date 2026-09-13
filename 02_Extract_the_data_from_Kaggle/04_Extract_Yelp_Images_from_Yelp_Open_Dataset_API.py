# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Notebook Title
# MAGIC %md
# MAGIC # Extract Yelp Images Dataset from Yelp Open Dataset API
# MAGIC
# MAGIC Creates the `yelp_images_dataset` folder in the Bronze landing zone and (once Yelp Open Dataset API access is granted) downloads the images dataset into it.
# MAGIC
# MAGIC > **Status:** Yelp Open Dataset API access pending — only the folder creation step is active. All other cells are commented out for future use.
# MAGIC >
# MAGIC > **Note:** This notebook will use the **Yelp Open Dataset website API** (not Kaggle) to download the images. The extraction logic will be updated once API access is obtained.

# COMMAND ----------

# DBTITLE 1,Install dependencies (commented out — pending Yelp Open Dataset API access)
# -----------------------------------------------------------------------
# TODO: Uncomment and update once Yelp Open Dataset API access is granted.
#       Replace kagglehub with the appropriate Yelp API client library.
# -----------------------------------------------------------------------
# %pip install python-dotenv requests --quiet
# dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Tip: Restart Python after install
# MAGIC %md
# MAGIC > **Tip (apply once API is accessed):** Always call `restartPython()` after `%pip install` to ensure newly installed packages load cleanly. Without this, cached module states from previous runs can cause subtle import conflicts.
# MAGIC >
# MAGIC > *Revise this note after Yelp Open Dataset API access is obtained — the exact dependencies may change.*

# COMMAND ----------

# DBTITLE 1,Tip: Credential security
# MAGIC %md
# MAGIC > **Tip (apply once API is accessed):** Load API credentials from the `.env` file (excluded from Git via `.gitignore`) — never hardcode tokens in notebook cells. The Yelp API may use a different auth method than Kaggle (e.g., OAuth2 or an API key header), so update the `.env` variable names and the credential-loading logic accordingly.
# MAGIC >
# MAGIC > *Revise this note after Yelp Open Dataset API access is obtained — the auth flow and env variable names will need updating.*

# COMMAND ----------

# DBTITLE 1,Tip: Idempotency and cache cleanup
# MAGIC %md
# MAGIC > **Tip (apply once API is accessed):** Ensure the download cell is **idempotent** — use file-overwrite semantics (like `shutil.copy2()`) so re-runs never create duplicates. Also **clean up any local cache** after files are verified in the volume to free disk space on serverless compute, where local storage is limited and shared.
# MAGIC >
# MAGIC > *Revise this note after Yelp Open Dataset API access is obtained — the download and caching mechanism may differ from Kaggle’s.*

# COMMAND ----------

# DBTITLE 1,Configure Yelp API Credentials (commented out — pending Yelp Open Dataset API access)
# -----------------------------------------------------------------------
# TODO: Uncomment once Yelp Open Dataset API access is granted.
#       Update the env variable name and auth method to match the
#       Yelp Open Dataset API requirements (may differ from Kaggle).
# -----------------------------------------------------------------------
# import os
# import pathlib
# from dotenv import load_dotenv
#
# # Load variables from the project-root .env file
# project_root = os.path.dirname(os.getcwd())
# env_path = os.path.join(project_root, ".env")
# load_dotenv(env_path, override=True)
#
# kaggle_token = os.environ.get("KAGGLE_API_TOKEN")
# if not kaggle_token or kaggle_token.startswith("<"):
#     raise ValueError(
#         f"KAGGLE_API_TOKEN not set. Open {env_path} and paste your KGAT_* token."
#     )
#
# # Write to ~/.kaggle/access_token (fallback for CLI & older libs)
# kaggle_dir = pathlib.Path.home() / ".kaggle"
# kaggle_dir.mkdir(parents=True, exist_ok=True)
# access_token_path = kaggle_dir / "access_token"
# access_token_path.write_text(kaggle_token)
# access_token_path.chmod(0o600)
#
# print("Kaggle API token loaded securely from .env file.")
# print(f"  - KAGGLE_API_TOKEN env var: set")
# print(f"  - {access_token_path}: written (chmod 600)")

# COMMAND ----------

# DBTITLE 1,Create yelp_images_dataset folder in Landing Zone
import os

# --------------------------------------------------
# Create the yelp_images_dataset folder inside the
# Bronze landing_zone volume (active — no API needed)
# --------------------------------------------------
volume_path = "/Volumes/yelp_poc_uc/bronze/landing_zone/yelp_images_dataset"
os.makedirs(volume_path, exist_ok=True)

print(f"Folder created: {volume_path}")
print(f"  Exists: {os.path.isdir(volume_path)}")

# COMMAND ----------

# DBTITLE 1,Download Yelp Images to Landing Zone (commented out — pending Yelp Open Dataset API access)
# -----------------------------------------------------------------------
# TODO: Rewrite once Yelp Open Dataset API access is granted.
#       This cell should be replaced with Yelp Open Dataset API calls
#       to download the images dataset (NOT Kaggle). The target folder
#       is already created in the cell above.
#
#       Yelp Open Dataset: https://www.yelp.com/dataset
#       The download logic, authentication, and file handling will
#       differ from the Kaggle approach used in notebook 03.
# -----------------------------------------------------------------------
# import requests
# import shutil
#
# volume_path = "/Volumes/yelp_poc_uc/bronze/landing_zone/yelp_images_dataset"
#
# # Placeholder — replace with Yelp Open Dataset API download logic
# # response = requests.get("<YELP_OPEN_DATASET_DOWNLOAD_URL>", ...)
# # ...

# COMMAND ----------

# DBTITLE 1,Workaround & Next Steps
# MAGIC %md
# MAGIC ## Temporary Workaround (Manual Upload)
# MAGIC
# MAGIC Since Yelp Open Dataset API access is not yet available, the following manual steps were taken:
# MAGIC
# MAGIC 1. **Downloaded locally** — The Yelp images dataset was downloaded directly from the [Yelp Open Dataset](https://www.yelp.com/dataset) website onto a local machine.
# MAGIC 2. **Uploaded to Databricks** — The downloaded images were then manually uploaded into the folder created above: `/Volumes/yelp_poc_uc/bronze/landing_zone/yelp_images_dataset`.
# MAGIC 3. **Proceeding with the project** — The rest of the pipeline (Bronze → Silver → Gold) will continue using the manually uploaded data. The automated extraction logic in this notebook will be completed once Yelp API access is obtained.
# MAGIC
# MAGIC > This notebook should be revisited and fully automated once Yelp Open Dataset API credentials are available.