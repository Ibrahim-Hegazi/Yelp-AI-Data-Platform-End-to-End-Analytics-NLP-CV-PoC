# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # 00 - Setup Service Principal & Groups
# MAGIC
# MAGIC ## Purpose
# MAGIC This notebook creates the **identity layer** for your Databricks workspace:  
# MAGIC a **Databricks-managed Service Principal** and **groups** with assigned members.  
# MAGIC This prevents the scenario where a user's account is deactivated and all resources become inaccessible.
# MAGIC
# MAGIC > **Separation of concerns:** This notebook handles identities only (SP + groups).  
# MAGIC > Catalog creation, schema setup, ownership transfer, and privilege grants live in **`01_Catalog_Schema_and_Grants`**.
# MAGIC
# MAGIC ## What This Notebook Does (in order)
# MAGIC 1. **Creates a Service Principal** — the non-human identity that will own all catalogs/schemas
# MAGIC 2. **Creates Groups** — with configurable names and descriptions
# MAGIC 3. **Adds users to groups** — based on the membership config
# MAGIC 4. **Adds the SP to the workspace admins group** — so it can own Unity Catalog objects
# MAGIC 5. **Verifies** the setup
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - You must be a **Workspace Admin**
# MAGIC - Run cells **in order**, one section at a time
# MAGIC - **Review each cell's output** before proceeding to the next
# MAGIC
# MAGIC ## Idempotency
# MAGIC Every cell is idempotent — safe to re-run without creating duplicates.

# COMMAND ----------

# DBTITLE 1,Configuration - REVIEW AND EDIT BEFORE RUNNING
# ============================================================
# CONFIGURATION - REVIEW AND EDIT THIS SECTION BEFORE RUNNING
# ============================================================
# All variables are here. To add/remove people or groups,
# edit ONLY this cell — the rest of the notebook reads from here.
# ============================================================

# ---- Service Principal ----
SP_DISPLAY_NAME = "sp-catalog-admin"
ADD_SP_TO_ADMINS = True  # SP needs admin to own UC objects

# ---- Groups ----
# Format: {"group-name": "description"}
# Add new groups by adding a new key-value pair.
GROUPS = {
    "data-engineers": "Data engineering team - read/write access to data schemas",
    "ai-engineers":   "AI/ML engineering team - read access to curated data, write to ML schemas",
    "project-admins": "Project leads - elevated privileges, but NOT catalog owners",
}

# ---- Group Members ----
# Format: {"group-name": ["user1@email.com", "user2@email.com"]}
# Add new members by appending emails to the list.
# Every group defined above MUST appear here (even if empty).
GROUP_MEMBERS = {
    "data-engineers": [
        "ibrahim.mohamed@gbrands.com",
        "habiba.ibrahim@gbrands.com",
        # "second.engineer@gbrands.com",
    ],
    "ai-engineers": [
        "mohamed.mostafa@gbrands.com",
        "ziad.sakr@gbrands.com",
        # "ai.engineer@gbrands.com",
    ],
    "project-admins": [
        "mohamed.mostafa@gbrands.com",
        "ibrahim.mohamed@gbrands.com",
    ],
}

# ---- Summary ----
print("Configuration loaded. Review the values above before proceeding.")
print(f"  Service Principal : {SP_DISPLAY_NAME}")
print(f"  Add SP to admins : {ADD_SP_TO_ADMINS}")
print(f"  Groups to create : {list(GROUPS.keys())}")
for g, members in GROUP_MEMBERS.items():
    print(f"    {g}: {members if members else '(empty)'}")

# COMMAND ----------

# DBTITLE 1,Helper Functions (API Utilities)
import requests
import json

# Setup API client
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

def _get(path):
    return _api("GET", path)

def _post(path, payload):
    return _api("POST", path, payload)

def _patch(path, payload):
    return _api("PATCH", path, payload)

def _put(path, payload):
    return _api("PUT", path, payload)

print("API helper functions loaded.")
print(f"Workspace: {_HOST}")

# COMMAND ----------

# DBTITLE 1,Step 1: Create Service Principal
# ============================================================
# STEP 1: Create the Databricks-Managed Service Principal
# ============================================================
# Idempotent: checks if SP already exists before creating

def get_or_create_service_principal(display_name):
    """Create SP if it doesn't exist, return its details."""
    # Check if SP already exists
    existing = _get("/api/2.0/preview/scim/v2/ServicePrincipals")
    if existing:
        for sp in existing.get("Resources", []):
            if sp.get("displayName") == display_name:
                print(f"  Service Principal '{display_name}' already exists (ID: {sp['id']})")
                return sp

    # Create new SP
    payload = {
        "displayName": display_name,
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:ServicePrincipal"],
        "active": True,
    }
    result = _post("/api/2.0/preview/scim/v2/ServicePrincipals", payload)
    if result:
        print(f"  Created Service Principal '{display_name}' (ID: {result['id']})")
        print(f"  Application ID: {result.get('applicationId', 'N/A')}")
    return result

sp = get_or_create_service_principal(SP_DISPLAY_NAME)
if sp:
    SP_ID = sp["id"]
    SP_APP_ID = sp.get("applicationId", "N/A")
    print(f"\n  SP Internal ID: {SP_ID}")
    print(f"  SP Application ID: {SP_APP_ID}")
    print("\n  [OK] Service Principal is ready.")
else:
    raise Exception("Failed to create Service Principal. Check if you have admin permissions.")

# COMMAND ----------

# DBTITLE 1,Step 2: Create Groups (Account-Level)
# ============================================================
# STEP 2: Create Groups at ACCOUNT LEVEL
# ============================================================
# Unity Catalog only recognizes account-level groups for GRANT.
# Workspace-local groups (created via /preview/scim) are invisible to UC.
# This cell uses the account-level SCIM API instead.
# Idempotent: checks if each group already exists before creating.

ACCOUNT_SCIM = "/api/2.0/account/scim/v2"

def get_or_create_account_group(group_name):
    """Create an account-level group if it doesn't exist."""
    existing = _get(f"{ACCOUNT_SCIM}/Groups")
    if existing:
        for g in existing.get("Resources", []):
            if g.get("displayName") == group_name:
                print(f"  Account group '{group_name}' already exists (ID: {g['id']})")
                return g

    payload = {
        "displayName": group_name,
        "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
    }
    result = _post(f"{ACCOUNT_SCIM}/Groups", payload)
    if result:
        print(f"  Created account group '{group_name}' (ID: {result['id']})")
    return result

group_ids = {}
for group_name, description in GROUPS.items():
    g = get_or_create_account_group(group_name)
    if g:
        group_ids[group_name] = g["id"]

print(f"\n  [OK] Account-level groups ready: {list(group_ids.keys())}")

# COMMAND ----------

# DBTITLE 1,Step 2b: Add Users to Groups (Account-Level)
# ============================================================
# STEP 2b: Add Users to Groups (Account-Level)
# ============================================================
# Uses account-level SCIM to look up users and manage membership.
# Idempotent: checks existing membership before adding.

def get_account_user_id(username):
    """Look up a user's account-level SCIM ID by email."""
    resp = _get(f"{ACCOUNT_SCIM}/Users?filter=userName eq \"{username}\"")
    if resp and resp.get("Resources"):
        return resp["Resources"][0]["id"]
    print(f"  [WARN] User '{username}' not found at account level. Skipping.")
    return None

def add_member_to_account_group(group_id, member_id, member_name, group_name):
    """Add a user to an account-level group if not already a member."""
    group_detail = _get(f"{ACCOUNT_SCIM}/Groups/{group_id}")
    if group_detail:
        current_members = [m.get("value") for m in group_detail.get("members", [])]
        if member_id in current_members:
            print(f"  User '{member_name}' is already in group '{group_name}'. Skipping.")
            return True

    payload = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [{
            "op": "add",
            "value": {"members": [{"value": member_id}]}
        }]
    }
    result = _patch(f"{ACCOUNT_SCIM}/Groups/{group_id}", payload)
    if result is not None:
        print(f"  Added '{member_name}' to account group '{group_name}'")
        return True
    return False

# Process all group memberships
for group_name, members in GROUP_MEMBERS.items():
    if group_name not in group_ids:
        print(f"  [WARN] Group '{group_name}' not found. Skipping its members.")
        continue
    print(f"\nProcessing group: {group_name}")
    for username in members:
        user_id = get_account_user_id(username)
        if user_id:
            add_member_to_account_group(group_ids[group_name], user_id, username, group_name)

print("\n  [OK] Account-level group memberships configured.")

# COMMAND ----------

# DBTITLE 1,Step 3: Add Service Principal to Workspace Admins
# ============================================================
# STEP 3: Add Service Principal to Workspace Admins
# ============================================================
# The SP needs workspace access to own Unity Catalog objects.
# We add it to the 'admins' group so it can manage catalogs.
# This is required for ownership transfer.
# Controlled by ADD_SP_TO_ADMINS flag in the config cell.

def get_admins_group_id():
    """Find the built-in 'admins' group ID."""
    existing = _get("/api/2.0/preview/scim/v2/Groups")
    if existing:
        for g in existing.get("Resources", []):
            if g.get("displayName") == "admins":
                return g["id"]
    return None

if not ADD_SP_TO_ADMINS:
    print("  ADD_SP_TO_ADMINS is False. Skipping.")
else:
    admins_group_id = get_admins_group_id()
    if not admins_group_id:
        raise Exception("Could not find 'admins' group.")
    # Check if SP is already in admins
    group_detail = _get(f"/api/2.0/preview/scim/v2/Groups/{admins_group_id}")
    current_members = [m.get("value") for m in group_detail.get("members", [])]

    if SP_ID in current_members:
        print(f"  SP '{SP_DISPLAY_NAME}' is already in 'admins' group.")
    else:
        payload = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{
                "op": "add",
                "value": {"members": [{"value": SP_ID}]}
            }]
        }
        result = _patch(f"/api/2.0/preview/scim/v2/Groups/{admins_group_id}", payload)
        if result is not None:
            print(f"  Added SP '{SP_DISPLAY_NAME}' to 'admins' group.")

    print(f"\n  [OK] Service Principal has admin access.")

# COMMAND ----------

# DBTITLE 1,Step 4: Verification
# ============================================================
# STEP 4: Verification - Review the Final State
# ============================================================

print("=" * 60)
print("VERIFICATION REPORT — Identity Layer")
print("=" * 60)

# 1. Service Principal
print("\n--- Service Principal ---")
sp_check = _get("/api/2.0/preview/scim/v2/ServicePrincipals")
sp_found = False
for sp in sp_check.get("Resources", []):
    if sp.get("displayName") == SP_DISPLAY_NAME:
        sp_found = True
        print(f"  Name:           {sp['displayName']}")
        print(f"  Internal ID:    {sp['id']}")
        print(f"  Application ID: {sp.get('applicationId', 'N/A')}")
        print(f"  Active:         {sp.get('active', 'N/A')}")
if not sp_found:
    print(f"  [ERROR] SP '{SP_DISPLAY_NAME}' not found!")

# 2. Account-level groups and their members
#    The SCIM list endpoint doesn't embed members — must fetch each group by ID.
print("\n--- Account-Level Groups ---")
all_groups = _get(f"{ACCOUNT_SCIM}/Groups")
for g in all_groups.get("Resources", []):
    if g.get("displayName") in GROUPS:
        # Fetch full group details (includes members)
        detail = _get(f"{ACCOUNT_SCIM}/Groups/{g['id']}")
        members = detail.get("members", []) if detail else []
        print(f"  {g['displayName']}: {len(members)} member(s)")
        for m in members:
            print(f"    - {m.get('display', m.get('value', 'unknown'))}")

# 3. SP admin membership (admins is a workspace-level group)
if ADD_SP_TO_ADMINS:
    print("\n--- SP Admin Membership (workspace-level) ---")
    ws_groups = _get("/api/2.0/preview/scim/v2/Groups")
    for g in ws_groups.get("Resources", []):
        if g.get("displayName") == "admins":
            member_ids = [m.get("value") for m in g.get("members", [])]
            print(f"  SP in admins group: {SP_ID in member_ids}")

print("\n" + "=" * 60)
print("IDENTITY SETUP COMPLETE")
print("=" * 60)
print(f"\nService Principal '{SP_DISPLAY_NAME}' and groups are ready.")
print("\nNext step:")
print("  Run notebook 01_Catalog_Schema_and_Grants to create catalogs,")
print("  transfer ownership to the SP, and grant privileges to groups.")