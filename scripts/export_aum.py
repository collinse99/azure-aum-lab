#!/usr/bin/env python3
"""Export Azure VM inventory and Update Manager records to Blob Storage.

Run after Azure CLI sign-in. Set AZURE_STORAGE_ACCOUNT to the target account
and AZURE_SUBSCRIPTION_IDS to the comma-separated subscriptions to query.
BRONZE_PREFIX optionally changes the blob prefix (default: aum).
CI_PIPELINE_ID, supplied by the workflow, identifies the run in blob names.

The target container is aum-bronze. The signed-in identity needs access to
query each subscription and write blobs in that container. The script stages
JSON under out/ and publishes the manifest after all dataset uploads succeed.
"""
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

API = "https://management.azure.com/providers/Microsoft.ResourceGraph/resources?api-version=2022-10-01"
BRONZE_CONTAINER = "aum-bronze"

QUERIES = {
    "vm_inventory": (
        "Resources"
        " | where type =~ 'microsoft.compute/virtualmachines'"
        " | project id, name, type, subscriptionId, resourceGroup, location, properties, tags"
        " | order by id asc"
    ),
    "assessments": (
        "PatchAssessmentResources"
        " | project id, name, type, subscriptionId, resourceGroup, properties"
        " | order by id asc"
    ),
    "installations": (
        "PatchInstallationResources"
        " | project id, name, type, subscriptionId, resourceGroup, properties"
        " | order by id asc"
    ),
}

def az(*args):
    result = subprocess.run(["az", *args], check=True, capture_output=True, text=True)
    return result.stdout.strip()

def token():
    return az("account", "get-access-token", "--resource", "https://management.azure.com/", "--query", "accessToken", "-o", "tsv")

def subscriptions():
    """Verify that every requested subscription is enabled and visible."""
    raw = os.getenv("AZURE_SUBSCRIPTION_IDS", "")
    if not raw.strip():
        raise RuntimeError("Set AZURE_SUBSCRIPTION_IDS to the exact comma-separated subscription IDs expected in this export")
    result = list(dict.fromkeys(x.strip() for x in raw.split(",") if x.strip()))
    available = {r["id"] for r in json.loads(az("account", "list", "--all", "-o", "json")) if r.get("state") == "Enabled"}
    unavailable = sorted(set(result) - available)
    if unavailable:
        raise RuntimeError(f"These requested subscriptions are not enabled/visible to the CI identity: {unavailable}")
    return result

def request_page(body, bearer):
    """Fetch one Resource Graph page, retrying transient failures."""
    raw = json.dumps(body).encode("utf-8")
    for attempt in range(5):
        req = Request(API, data=raw, method="POST", headers={
            "Authorization": "Bearer " + bearer, "Content-Type": "application/json"})
        try:
            with urlopen(req, timeout=90) as response:
                return json.load(response)
        except HTTPError as err:
            if err.code not in (429, 500, 502, 503, 504) or attempt == 4:
                detail = err.read().decode("utf-8", errors="replace")[:1200]
                raise RuntimeError(f"ARG HTTP {err.code}: {detail}") from err
            retry_after = err.headers.get("Retry-After", "")
            seconds = int(retry_after) if retry_after.isdigit() else min(2 ** (attempt + 1), 30)
            time.sleep(min(seconds, 60))
        except URLError:
            if attempt == 4:
                raise
            time.sleep(min(2 ** (attempt + 1), 30))
    raise RuntimeError("Unreachable: all retries exhausted")

def query_all(query, scope, bearer):
    """Read every page and reject incomplete or looping pagination."""
    rows = []
    skip = None
    seen_tokens = set()
    while True:
        body = {"subscriptions": scope, "query": query, "options": {"$top": 1000, "resultFormat": "objectArray"}}
        if skip:
            body["options"]["$skipToken"] = skip
        page = request_page(body, bearer)
        data = page.get("data")
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected response from Resource Graph: {str(page)[:400]}")
        rows.extend(data)
        new_skip = page.get("$skipToken")
        if new_skip:
            if new_skip in seen_tokens:
                raise RuntimeError("Resource Graph returned a repeated pagination token")
            seen_tokens.add(new_skip)
            skip = new_skip
            continue
        if str(page.get("resultTruncated", "false")).lower() == "true":
            raise RuntimeError("Resource Graph says result truncated but returned no continuation token")
        total = page.get("totalRecords")
        if isinstance(total, int) and total != len(rows):
            raise RuntimeError(f"Pagination count mismatch: got {len(rows)}, expected {total}")
        return rows

def upload(account, container, blob_name, file_path):
    az("storage", "blob", "upload", "--account-name", account, "--container-name", container,
       "--name", blob_name, "--file", str(file_path), "--overwrite", "true", "--auth-mode", "login", "--only-show-errors", "-o", "none")

def main():
    account = os.environ["AZURE_STORAGE_ACCOUNT"]
    subs = subscriptions()
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%dT%H-%M-%SZ")
    day = now.strftime("%Y-%m-%d")
    run = os.getenv("CI_PIPELINE_ID", "manual")
    prefix = os.getenv("BRONZE_PREFIX", "aum").strip("/")
    folder = Path("out")
    folder.mkdir(exist_ok=True)
    bearer = token()
    exported_at = now.isoformat()
    manifest = {
        "exported_at_utc": exported_at,
        "run": run,
        "subscriptions": subs,
        "container": BRONZE_CONTAINER,
        "datasets": {},
    }
    # Query one subscription at a time to make subscription coverage and failures auditable.
    for sub in subs:
        for key, query in QUERIES.items():
            print(f"Querying {key} in subscription {sub}", flush=True)
            rows = query_all(query, [sub], bearer)
            name = f"{prefix}/{key}/date={day}/subscription={sub}/run={run}-{stamp}.json"
            payload = {
                "dataset": key,
                "subscription_id": sub,
                "exported_at_utc": exported_at,
                "count": len(rows),
                "records": rows,
            }
            path = folder / f"{key}-{sub}.json"
            path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
            upload(account, BRONZE_CONTAINER, name, path)
            manifest["datasets"].setdefault(key, []).append(
                {"subscription": sub, "blob": name, "count": len(rows)}
            )
            print(f"Uploaded {BRONZE_CONTAINER}/{name} ({len(rows)} rows)", flush=True)
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    # Publish manifest last. A missing manifest indicates an incomplete run.
    manifest_name = f"{prefix}/manifests/date={day}/run={run}-{stamp}.json"
    upload(account, BRONZE_CONTAINER, manifest_name, folder / "manifest.json")
    print(f"SUCCESS: complete manifest at {BRONZE_CONTAINER}/{manifest_name}", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("EXPORT FAILED:", exc, file=sys.stderr)
        sys.exit(1)
