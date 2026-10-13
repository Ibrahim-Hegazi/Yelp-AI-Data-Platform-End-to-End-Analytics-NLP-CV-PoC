# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 01 - Catalog, Schema & Grants
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook creates and manages the **data infrastructure layer**: catalogs, schemas, volumes, ownership, and privilege grants.  
# MAGIC All objects are transferred to the **Service Principal** created in `00_Setup_Service_Principal_And_Groups`.
# MAGIC
# MAGIC > **Prerequisite:** Run `00_Setup_Service_Principal_And_Groups` first to create the SP and groups.
# MAGIC
# MAGIC ## What This Notebook Does (in order)
# MAGIC 1. **Creates Catalogs** — as defined in the config
# MAGIC 2. **Creates Schemas** — bronze_landing / bronze_staging / bronze_final / Silver / Gold (or whatever you define)
# MAGIC 3. **Creates Volumes** — landing zones for raw data
# MAGIC 4. **Grants Privileges** — to each group (must happen while you still own the catalog)
# MAGIC 5. **Transfers Ownership** — catalogs, schemas, and volumes to the SP (after grants)
# MAGIC 6. **Verifies** the final state
# MAGIC
# MAGIC ## When to Run
# MAGIC - **First time:** After running notebook 00
# MAGIC - **New project:** Add a new catalog block to the config and re-run
# MAGIC - **Recovery:** If an engineer accidentally changed ownership or grants, re-run to restore
# MAGIC
# MAGIC ## Idempotency
# MAGIC Every cell is idempotent — `IF NOT EXISTS` for creation, ownership checks before transfer, and `GRANT` is a no-op if already granted.

# COMMAND ----------

# DBTITLE 1,Configuration - REVIEW AND EDIT BEFORE RUNNING
# ============================================================
# CONFIGURATION - REVIEW AND EDIT THIS SECTION BEFORE RUNNING
# ============================================================
# All variables are here. To add a new project/catalog, copy
# one of the catalog blocks and modify it. To change grants,
# edit GROUP_PRIVILEGES. The rest of the notebook reads from here.
# ============================================================

# ---- Service Principal (must match notebook 00) ----
SP_OWNER = "sp-catalog-admin"

# ---- Catalogs, Schemas, and Volumes ----
# Each entry defines one catalog and everything inside it.
# To add a new project, just add a new dict to this list.
CATALOGS = [
    {
        "name": "yelp_poc_uc",
        "comment": "Yelp POC Unity Catalog - end-to-end analytics, NLP, and CV",
        "schemas": [
            {"name": "bronze_landing", "comment": "Raw ingested data landing zone — stores untouched source files as-is before any processing"},
            {"name": "bronze_staging", "comment": "Schema-validated data — clean and error rows separated as managed Delta tables for review and remediation"},
            {"name": "bronze_final",   "comment": "Reconciled bronze data — clean and remediated rows combined into final Delta tables ready for Silver enrichment"},
            {"name": "Silver",         "comment": "Cleansed and enriched data with applied business rules and quality checks"},
            {"name": "Gold",           "comment": "Curated analytics-ready tables optimised for reporting and ML consumption"},
        ],
        "volumes": [
            {"schema": "bronze_landing", "name": "landing_zone", "comment": "Volume for storing initial raw data extracted from main Yelp data sources"},
        ],
    },
    # ---- Add new projects below ----
    # {
    #     "name": "new_project_uc",
    #     "comment": "Description of the new project",
    #     "schemas": [
    #         {"name": "Bronze", "comment": "Raw data"},
    #         {"name": "Silver", "comment": "Cleansed data"},
    #         {"name": "Gold",   "comment": "Analytics-ready data"},
    #     ],
    #     "volumes": [
    #         {"schema": "Bronze", "name": "landing_zone", "comment": "Raw file landing zone"},
    #     ],
    # },
]

# ---- Privileges per Group ----
# These are applied to EVERY catalog above.
# Format: {"group-name": ["PRIVILEGE1", "PRIVILEGE2"]}
GROUP_PRIVILEGES = {
    "data-engineers": [
        "USE CATALOG", "USE SCHEMA", "SELECT", "MODIFY",
        "CREATE TABLE", "CREATE SCHEMA",
    ],
    "ai-engineers": [
        "USE CATALOG", "USE SCHEMA", "SELECT",
    ],
    "project-admins": [
        "ALL PRIVILEGES",
        "EXTERNAL USE SCHEMA",
        "MANAGE",
    ],
}

# ---- Summary ----
print("Configuration loaded. Review the values above before proceeding.")
print(f"  SP Owner : {SP_OWNER}")
print(f"  Catalogs : {[c['name'] for c in CATALOGS]}")
for c in CATALOGS:
    print(f"    {c['name']}:")
    print(f"      Schemas : {[s['name'] for s in c['schemas']]}")
    print(f"      Volumes : {[v['name'] for v in c['volumes']]}")
print(f"  Groups with privileges: {list(GROUP_PRIVILEGES.keys())}")

# COMMAND ----------

# DBTITLE 1,API Helpers
import requests
import json

_HOST = spark.conf.get("spark.databricks.workspaceUrl")
_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
_HEADERS = {"Authorization": f"Bearer {_TOKEN}", "Content-Type": "application/json"}

def _api(method, path, payload=None):
    """Generic API caller with error handling."""
    url = f"https://{_HOST}{path}"
    resp = requests.request(method, url, headers=_HEADERS, json=payload)
    if resp.status_code not in (200, 201, 204):
        print(f"  [ERROR] {method} {path} -> {resp.status_code}: {resp.text}")
        return None
    # 204 = No Content (success for PATCH/DELETE with no response body)
    if resp.status_code == 204 or not resp.text:
        return {}
    return resp.json()

def _get(path):   return _api("GET", path)
def _post(path, payload):  return _api("POST", path, payload)
def _patch(path, payload): return _api("PATCH", path, payload)

print(f"API helpers loaded. Workspace: {_HOST}")

# COMMAND ----------

# DBTITLE 1,Validate Service Principal Exists
# ============================================================
# PRE-CHECK: Confirm the Service Principal from notebook 00 exists
# ============================================================

# Unity Catalog identifies service principals by their Application ID (UUID),
# NOT by display name. This cell resolves the display name to the App ID
# so that ownership transfer and GRANT statements work correctly.

sp_resp = _get("/api/2.0/preview/scim/v2/ServicePrincipals")
SP_APP_ID = None
if sp_resp:
    for sp in sp_resp.get("Resources", []):
        if sp.get("displayName") == SP_OWNER:
            SP_APP_ID = sp.get("applicationId")
            print(f"  [OK] SP '{SP_OWNER}' found")
            print(f"       Internal ID:    {sp['id']}")
            print(f"       Application ID: {SP_APP_ID}  <-- used for ownership & grants")
            break

if not SP_APP_ID:
    raise Exception(
        f"Service Principal '{SP_OWNER}' not found. "
        f"Run notebook 00_Setup_Service_Principal_And_Groups first."
    )

# COMMAND ----------

# DBTITLE 1,Step 1: Create Catalogs
# ============================================================
# STEP 1: Create Catalogs
# ============================================================

for cat in CATALOGS:
    cat_name = cat["name"]
    cat_comment = cat.get("comment", "")
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS `{cat_name}` COMMENT '{cat_comment}'")
        print(f"  [OK] Catalog '{cat_name}' ready.")
    except Exception as e:
        print(f"  [ERROR] Catalog '{cat_name}': {e}")

print("\n  Step 1 complete.")

# COMMAND ----------

# DBTITLE 1,Step 2: Create Schemas
# ============================================================
# STEP 2: Create Schemas inside each Catalog
# ============================================================

for cat in CATALOGS:
    cat_name = cat["name"]
    print(f"\n--- {cat_name} ---")
    for schema in cat.get("schemas", []):
        schema_name = schema["name"]
        schema_comment = schema.get("comment", "")
        full_name = f"`{cat_name}`.`{schema_name}`"
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_name} COMMENT '{schema_comment}'")
            print(f"  [OK] Schema {full_name} ready.")
        except Exception as e:
            print(f"  [ERROR] Schema {full_name}: {e}")

print("\n  Step 2 complete.")

# COMMAND ----------

# DBTITLE 1,Step 3: Create Volumes
# ============================================================
# STEP 3: Create Volumes inside their respective Schemas
# ============================================================

for cat in CATALOGS:
    cat_name = cat["name"]
    volumes = cat.get("volumes", [])
    if not volumes:
        continue
    print(f"\n--- {cat_name} ---")
    for vol in volumes:
        schema_name = vol["schema"]
        vol_name = vol["name"]
        vol_comment = vol.get("comment", "")
        full_name = f"`{cat_name}`.`{schema_name}`.`{vol_name}`"
        try:
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {full_name} COMMENT '{vol_comment}'")
            print(f"  [OK] Volume {full_name} ready.")
        except Exception as e:
            print(f"  [ERROR] Volume {full_name}: {e}")

print("\n  Step 3 complete.")

# COMMAND ----------

# DBTITLE 1,Step 4: Grant Privileges to Groups (BEFORE ownership transfer)
# ============================================================
# STEP 4: Grant Privileges to Groups on All Catalogs
# ============================================================
# MUST run BEFORE ownership transfer — you need MANAGE privilege
# on the catalog to GRANT, and you lose that when ownership moves to the SP.
# GRANT is idempotent — re-granting an existing privilege is a no-op.
#
# If SQL GRANT fails (e.g. ownership already transferred), falls back to
# the UC Permissions REST API which may work with workspace admin tokens.

# Mapping from SQL privilege names to UC API privilege names
_SQL_TO_API = {
    "USE CATALOG": "USE_CATALOG",
    "USE SCHEMA": "USE_SCHEMA",
    "SELECT": "SELECT",
    "MODIFY": "MODIFY",
    "CREATE TABLE": "CREATE_TABLE",
    "CREATE SCHEMA": "CREATE_SCHEMA",
    "CREATE FUNCTION": "CREATE_FUNCTION",
    "ALL PRIVILEGES": "ALL_PRIVILEGES",
}

def _grant_via_api(cat_name, group_name, privileges):
    """Fallback: grant privileges via UC Permissions REST API."""
    api_privs = [_SQL_TO_API.get(p, p.replace(" ", "_")) for p in privileges]
    payload = {
        "changes": [{
            "principal": group_name,
            "add": api_privs,
        }]
    }
    result = _patch(f"/api/2.1/unity-catalog/permissions/catalog/{cat_name}", payload)
    return result is not None

for cat in CATALOGS:
    cat_name = cat["name"]
    print(f"\n--- {cat_name} ---")
    for group_name, privileges in GROUP_PRIVILEGES.items():
        sql_failed = False
        for priv in privileges:
            try:
                spark.sql(f"GRANT {priv} ON CATALOG `{cat_name}` TO `{group_name}`")
                print(f"  Granted {priv} to {group_name}")
            except Exception as e:
                if "PERMISSION_DENIED" in str(e) or "MANAGE" in str(e):
                    sql_failed = True
                    break  # No point trying more SQL grants — all will fail
                print(f"  [WARN] {priv} to {group_name}: {e}")

        # Fallback: if SQL GRANT failed due to PERMISSION_DENIED, try REST API
        if sql_failed:
            print(f"  SQL GRANT failed (no MANAGE). Trying REST API fallback...")
            if _grant_via_api(cat_name, group_name, privileges):
                print(f"  [OK] Granted {privileges} to {group_name} via REST API")
            else:
                print(f"  [ERROR] REST API also failed for {group_name}.")
                print(f"          You need metastore admin access to fix this.")
                print(f"          Ask your IT admin to make you a metastore admin,")
                print(f"          or generate an OAuth secret for the SP to run grants.")

print("\n  Step 4 complete.")

# COMMAND ----------

# DBTITLE 1,Step 5: Transfer Ownership to Service Principal (AFTER grants)
# ============================================================
# STEP 5: Transfer Ownership of All Objects to the SP
# ============================================================
# This is the KEY step. After this, the SP owns everything.
# If any user's account is deactivated, the objects persist.
# MUST run AFTER grants — once ownership transfers, you lose MANAGE.

def transfer_ownership(object_type, full_name, new_owner):
    """Transfer ownership via UC API. Returns True on success."""
    api_paths = {
        "catalog": f"/api/2.1/unity-catalog/catalogs/{full_name}",
        "schema":  f"/api/2.1/unity-catalog/schemas/{full_name}",
        "volume":  f"/api/2.1/unity-catalog/volumes/{full_name}",
    }
    path = api_paths.get(object_type)
    if not path:
        print(f"  [ERROR] Unknown object type: {object_type}")
        return False

    info = _get(path)
    if not info:
        print(f"  [WARN] {object_type} '{full_name}' not found. Skipping.")
        return False

    current_owner = info.get("owner", "unknown")
    if current_owner == new_owner:
        print(f"  {object_type} '{full_name}' already owned by '{new_owner}'. Skipping.")
        return True

    print(f"  Transferring {object_type} '{full_name}': {current_owner} -> {new_owner}")
    result = _patch(path, {"owner": new_owner})
    if result:
        print(f"  [OK] Transferred.")
        return True
    return False

for cat in CATALOGS:
    cat_name = cat["name"]
    print(f"\n{'='*50}")
    print(f"Catalog: {cat_name}")
    print(f"{'='*50}")

    # Transfer catalog
    # Use SP_APP_ID (UUID) — UC identifies SPs by Application ID, not display name
    transfer_ownership("catalog", cat_name, SP_APP_ID)

    # Transfer schemas (skip information_schema)
    for schema in cat.get("schemas", []):
        full_name = f"{cat_name}.{schema['name']}"
        transfer_ownership("schema", full_name, SP_APP_ID)

    # Transfer volumes
    for vol in cat.get("volumes", []):
        full_name = f"{cat_name}.{vol['schema']}.{vol['name']}"
        transfer_ownership("volume", full_name, SP_APP_ID)

print("\n  Step 5 complete.")

# COMMAND ----------

# DBTITLE 1,Step 6: Verification
# ============================================================
# STEP 6: Verification - Review the Final State
# ============================================================

print("=" * 60)
print("VERIFICATION REPORT — Data Infrastructure")
print("=" * 60)

for cat in CATALOGS:
    cat_name = cat["name"]
    print(f"\n{'='*50}")
    print(f"Catalog: {cat_name}")
    print(f"{'='*50}")

    # Catalog ownership
    cat_info = _get(f"/api/2.1/unity-catalog/catalogs/{cat_name}")
    if cat_info:
        owner = cat_info.get("owner", "N/A")
        status = "OK" if owner == SP_APP_ID else "MISMATCH"
        print(f"  Owner: {owner}  [{status}]")

    # Schema ownership
    schemas = _get(f"/api/2.1/unity-catalog/schemas?catalog_name={cat_name}")
    if schemas:
        for s in schemas.get("schemas", []):
            if s["name"] == "information_schema":
                continue
            owner = s.get("owner", "N/A")
            status = "OK" if owner == SP_APP_ID else "MISMATCH"
            print(f"  Schema {s['full_name']}: Owner = {owner}  [{status}]")

    # Volume ownership
    for vol in cat.get("volumes", []):
        vol_full = f"{cat_name}.{vol['schema']}.{vol['name']}"
        vol_info = _get(f"/api/2.1/unity-catalog/volumes/{vol_full}")
        if vol_info:
            owner = vol_info.get("owner", "N/A")
            status = "OK" if owner == SP_APP_ID else "MISMATCH"
            print(f"  Volume {vol_full}: Owner = {owner}  [{status}]")

    # Grants
    print(f"\n  Grants on {cat_name}:")
    try:
        grants = spark.sql(f"SHOW GRANTS ON CATALOG `{cat_name}`").collect()
        for row in grants:
            print(f"    {row['Principal']} -> {row['ActionType']}")
    except Exception as e:
        print(f"    [WARN] Cannot show grants: {e}")

print("\n" + "=" * 60)
print("DATA INFRASTRUCTURE SETUP COMPLETE")
print("=" * 60)
print(f"\nAll catalogs/schemas/volumes are owned by SP '{SP_OWNER}' (AppID: {SP_APP_ID}).")
print("Groups have been granted their configured privileges.")
print("\nTo add a new project:")
print("  1. Add a new catalog block to the CATALOGS list in the config cell")
print("  2. Re-run this notebook")