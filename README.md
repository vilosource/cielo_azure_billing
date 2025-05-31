# Cielo Azure Billing

This project imports Azure cost CSV reports into a Django application and exposes
the data through a REST API. Each import is stored as a snapshot so historical
values remain immutable and can be compared over time.

## Quick start

1. Install dependencies with [Poetry](https://python-poetry.org/):
   ```bash
   poetry install
   ```
2. Apply migrations:
   ```bash
   poetry run python manage.py migrate
   ```
3. Run the development server:
   ```bash
   poetry run python manage.py runserver
   ```
4. Import a cost CSV file:
   ```bash
   poetry run python manage.py import_cost_csv --file path/to/file.csv
   ```
5. Populate `resource_name` for existing resources:
   ```bash
   poetry run python manage.py backfill_resource_name
   ```

A default admin user (`admin`/`admin`) is created on first run. Visit `/admin/`
to explore the imported data.

The API documentation is available at `/api/docs/` and requires authentication
unless `API_AUTH_DISABLED` is set to `True` in settings.

When requesting cost entries for a specific billing date via
`/api/cost-entries/?date=YYYY-MM-DD`, the API returns data from the most recent
snapshot that includes that date. Summary endpoints apply the same rule but
determine the newest snapshot per subscription for the selected day to avoid
duplicates.

To discover which billing dates are present, call
`/api/costs/available-report-dates/?month=YYYY-MM` (defaults to the current
month).

To list the snapshot report dates that can be queried for summaries, use
`/api/snapshots/available-report-dates/`.

## API Filtering

Cost entry and cost summary endpoints share a common set of query parameters.  
These filters make it easy to drill down into specific subscriptions, resources
or tagged costs.  The main options are:

- `date` – billing date (defaults to the latest available when omitted)
- `subscription_id` – filter by subscription
- `resource_group` – filter by resource group
- `location` – filter by Azure region
- `meter_category` – e.g. `Virtual Machines`, `Storage`
- `meter_subcategory` – optional deeper filter such as `Premium SSD`
- `pricing_model` – pricing model, e.g. `OnDemand`, `Spot`
- `publisher_name` – filter Azure Marketplace charges
- `resource_name` – filter by resource name
- `min_cost` / `max_cost` – filter by cost range
- `source_id` – filter results from a specific `BillingBlobSource`
- `tag_key` and `tag_value` – filter by resource tags

Legacy camelCase parameters like `resourceGroupName` are still accepted for
backwards compatibility on the cost entry endpoint.

## Documentation

Additional details are available in [`docs/SPEC.md`](docs/SPEC.md). Guidance for
adding new management commands can be found in
[`docs/DJANGO_COMMANDS.md`](docs/DJANGO_COMMANDS.md).
Information on the automated blob import system lives in
[`docs/BLOB_IMPORT.md`](docs/BLOB_IMPORT.md).
Filtering examples for the API are provided in
[`docs/CostsFilteringGuide.md`](docs/CostsFilteringGuide.md).
