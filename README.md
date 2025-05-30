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

A default admin user (`admin`/`admin`) is created on first run. Visit `/admin/`
to explore the imported data.

The API documentation is available at `/api/docs/` and requires authentication
unless `API_AUTH_DISABLED` is set to `True` in settings.

When requesting cost entries for a specific billing date via
`/api/cost-entries/?date=YYYY-MM-DD`, the API returns data from the most recent
snapshot that includes that date.

## API Filtering

Cost entry endpoints accept the following query parameters:

- `resourceGroupName`
- `subscriptionName`
- `meterCategory`
- `meterSubCategory`
- `serviceFamily`
- `resourceLocation`
- `chargeType`
- `pricingModel`
- `publisherName`
- `costCenter`
- `tags`

## Documentation

Additional details are available in [`docs/SPEC.md`](docs/SPEC.md). Guidance for
adding new management commands can be found in
[`docs/DJANGO_COMMANDS.md`](docs/DJANGO_COMMANDS.md).
