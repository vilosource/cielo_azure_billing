# Azure Blob Billing Import

This project supports automated fetching of Azure cost export files directly from
Azure Blob Storage. Sources are defined with the `BillingBlobSource` model which
stores the base folder to scan and metadata about each import attempt.

## Models

### BillingBlobSource
- `name` – friendly identifier used by CLI options
    - `base_folder` – root folder containing export runs
- `is_active` – enable/disable the source
- timestamps for last attempt and import plus a `status` field

### ImportSnapshot
Import snapshots now store a unique `run_id` from Azure manifests and a
`report_date` extracted from the same file. A reference to the originating
`BillingBlobSource` is also stored.

Query helpers on `ImportSnapshot.objects` provide shortcuts for fetching the
latest snapshot per subscription or for a specific day.

## Management Command

`python manage.py fetch_and_import_from_blob` reads all active
`BillingBlobSource` entries, discovers available export runs under the configured
`base_folder`, downloads the `manifest.json` and related csv.gz file and imports
the cost entries. Options allow limiting to specific source names, overwriting
existing snapshots and performing dry runs.

`python manage.py inspect_blob_source` lists discovered export runs without
downloading them. It accepts the same `--source-name` and optional
`--billing-period` arguments to filter results.

The command relies on the Azure SDK (`azure-identity` and `azure-storage-blob`)
using `DefaultAzureCredential` for authentication.
