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
## Import Workflow Sequence

1. Each active `BillingBlobSource` defines a `base_folder` that points to the root of an Azure export container.
2. When `fetch_and_import_from_blob` runs, it authenticates with Azure using `DefaultAzureCredential` and lists blobs under that folder (optionally filtered by `--billing-period`).
3. For every `manifest.json` discovered:
   - The file is downloaded and parsed to extract `run_id`, `endDate`, and the csv blob name.
   - If `run_id` already exists in `ImportSnapshot` (and `--overwrite` is not used) the run is skipped.
   - The related `.csv.gz` is downloaded, decompressed and passed to `CostCsvImporter`.
4. `CostCsvImporter` creates an `ImportSnapshot` for the run and processes each row of the CSV, creating or updating `Customer`, `Subscription`, `Resource` and `Meter` records and finally inserting `CostEntry` rows linked to that snapshot.
5. After a successful import the `BillingBlobSource` record is updated with `last_imported_at` and its `status` is set to `"imported"`. Errors are recorded in `status` and written to the log.
6. The `inspect_blob_source` command performs steps 1–3 without downloading the csv file so you can verify available runs before importing.
