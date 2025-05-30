# Azure Blob Billing Import

This project supports automated fetching of Azure cost export files directly from
Azure Blob Storage. Sources are defined with the `BillingBlobSource` model which
stores the path template, subscription and metadata about each import attempt.

## Models

### BillingBlobSource
- `subscription` – related subscription
- `name` – friendly identifier used by CLI options
- `path_template` – format string with `{billing_period}` and `{guid}`
- `guid` – export GUID from Azure
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
`BillingBlobSource` entries, downloads the `manifest.json` and related csv.gz
file for a billing period and imports the cost entries. Options allow limiting to
specific source names, overwriting existing snapshots and performing dry runs.

The command relies on the Azure SDK (`azure-identity` and `azure-storage-blob`)
using `DefaultAzureCredential` for authentication.
