# cielo\_azure\_billing - Cost Import & Analysis Django Project Specification

## Purpose

Develop a **self-contained Django project** to import daily Azure cost CSV reports into a structured database, enabling historical snapshots, cost tracking, and analytics. The application will include its own models, admin interface, views, and management commands.

## Project Structure

```
cielo_azure_billing/
├── cielo_azure_billing/              # Django project root
│   ├── settings.py
│   ├── urls.py
├── billing/                # Main app for models and logic
│   ├── models.py
│   ├── admin.py
│   ├── views.py
│   ├── serializers.py
│   ├── urls.py
│   ├── management/
│   │   └── commands/
│   │       └── import_cost_csv.py
│   ├── templates/
│   └── static/
```

## Key Features

* Self-contained Django app for cost import and analysis.
* Tracks each import as a historical snapshot.
* Provides admin upload, management command for automation.
* Offers filterable and comparable views of cost data by date, subscription, etc.
* Fully API-ready via DRF if extended.

## Core Models

### `ImportSnapshot`

```python
class ImportSnapshot(models.Model):
    snapshot_date = models.DateField(auto_now_add=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)
    file_name = models.CharField(max_length=255)
```

### `Customer`

```python
class Customer(models.Model):
    tenant_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=255, null=True, blank=True)
```

### `Subscription`

```python
class Subscription(models.Model):
    subscription_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=255)
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)
```

### `Resource`

```python
class Resource(models.Model):
    resource_id = models.TextField(unique=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    resource_group = models.CharField(max_length=255, null=True, blank=True)
    location = models.CharField(max_length=255, null=True, blank=True)
```

### `Meter`

```python
class Meter(models.Model):
    meter_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=255)
    category = models.CharField(max_length=255)
    subcategory = models.CharField(max_length=255, null=True, blank=True)
    service_family = models.CharField(max_length=255, null=True, blank=True)
    unit = models.CharField(max_length=64)
```

### `CostEntry`

```python
class CostEntry(models.Model):
    snapshot = models.ForeignKey(ImportSnapshot, on_delete=models.CASCADE)
    date = models.DateField(db_index=True)
    subscription = models.ForeignKey(Subscription, on_delete=models.CASCADE)
    resource = models.ForeignKey(Resource, on_delete=models.CASCADE)
    meter = models.ForeignKey(Meter, on_delete=models.CASCADE)
    cost_in_usd = models.DecimalField(max_digits=12, decimal_places=4)
    cost_in_billing_currency = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    billing_currency = models.CharField(max_length=10, null=True, blank=True)
    quantity = models.DecimalField(max_digits=12, decimal_places=4)
    unit_price = models.DecimalField(max_digits=12, decimal_places=6)
    payg_price = models.DecimalField(max_digits=12, decimal_places=6, null=True, blank=True)
    pricing_model = models.CharField(max_length=64, null=True, blank=True)
    charge_type = models.CharField(max_length=64, null=True, blank=True)
    publisher_name = models.CharField(max_length=255, null=True, blank=True)
    cost_center = models.CharField(max_length=255, null=True, blank=True)
    tags = models.JSONField(null=True, blank=True)

    class Meta:
        unique_together = (
            'snapshot',
            'date',
            'subscription',
            'resource',
            'meter',
            'quantity',
            'unit_price',
        )
```

## Import Logic

* Run via Django management command:

  ```bash
  python manage.py import_cost_csv --file path/to/file.csv
  ```
* Accepts a `--file` argument pointing to the CSV file to import.
* Reads and parses the CSV.
* Creates a new `ImportSnapshot` with the filename and import date.
* For each row:

  * Normalize or create related entities (`Customer`, `Subscription`, `Meter`, `Resource`).
  * Create a `CostEntry` linked to the `ImportSnapshot`.
* Supports re-importing the same file without affecting previous snapshots (immutable snapshot model).
* Logs import statistics (e.g., new rows added, duplicates skipped, errors).

### Example Management Command: `import_cost_csv.py`

```python
from django.core.management.base import BaseCommand
from billing.services import CostCsvImporter


class Command(BaseCommand):
    """Import an Azure cost CSV file."""

    def add_arguments(self, parser):
        parser.add_argument("--file", required=True, help="Path to the CSV file")

    def handle(self, *args, **options):
        importer = CostCsvImporter(options["file"])
        count = importer.import_file()
        self.stdout.write(self.style.SUCCESS(f"Imported {count} entries"))
```

The `CostCsvImporter` service encapsulates all parsing and database logic. It
normalizes dates and processes optional fields such as `serviceFamily`,
`publisherName` and `costCenter` while creating a new `ImportSnapshot` for every
execution.

* Run via Django management command:

  ```bash
  python manage.py import_cost_csv --file path/to/file.csv
  ```

* Accepts a `--file` argument pointing to the CSV file to import.

* Reads and parses the CSV.

* Creates a new `ImportSnapshot` with the filename and import date.

* For each row:

  * Normalize or create related entities (`Customer`, `Subscription`, `Meter`, `Resource`).
  * Create a `CostEntry` linked to the `ImportSnapshot`.

* Supports re-importing the same file without affecting previous snapshots (immutable snapshot model).

* Logs import statistics (e.g., new rows added, duplicates skipped, errors).

* Via Django management command: `python manage.py import_cost_csv --file path/to/file.csv`

* Reads and parses the CSV.

* Creates new `ImportSnapshot` entry.

* Normalizes and upserts lookup data (Customer, Subscription, Meter, etc.).

* Stores new `CostEntry` rows linked to the snapshot.

## Admin & Security

* Register all models (`ImportSnapshot`, `Customer`, `Subscription`, `Resource`, `Meter`, `CostEntry`) with the Django Admin interface.

* Enable list display and filtering on relevant fields such as `snapshot_date`, `date`, `subscription`, `resource`.

* Use `list_filter`, `search_fields`, and `readonly_fields` where appropriate to aid data browsing.

* Staff-only upload via Django Admin.

* Management command for automated/scripted import.

* Default API security via DRF Token Authentication.

* Use a SOLID-compliant authentication strategy:

  * Implement a custom authentication class (`ConditionalTokenAuthentication`) that defers to settings to disable authentication in development mode via an `API_AUTH_DISABLED` flag.

    ```python
    from rest_framework.authentication import TokenAuthentication
    from django.conf import settings

    class ConditionalTokenAuthentication(TokenAuthentication):
        def authenticate(self, request):
            if getattr(settings, 'API_AUTH_DISABLED', False):
                return (None, None)
            return super().authenticate(request)
    ```

  * Use a configuration provider (`EnvConfig`) to abstract away the environment logic, following Dependency Inversion:

    ```python
    class EnvConfig:
        @staticmethod
        def api_auth_disabled():
            from django.conf import settings
            return getattr(settings, 'API_AUTH_DISABLED', False)
    ```

* Allow selective public access to endpoints using DRF's `AllowAny` permission class on views that should be accessible without authentication.

* Alternatively, create a `PublicEndpointPermission` class that checks `settings.PUBLIC_API_PATHS` or view names to grant access dynamically:

  ```python
  from rest_framework.permissions import BasePermission
  from django.conf import settings

  class PublicEndpointPermission(BasePermission):
      def has_permission(self, request, view):
          allowed_paths = getattr(settings, 'PUBLIC_API_PATHS', [])
          allowed_names = getattr(settings, 'PUBLIC_API_NAMES', [])
          return request.path in allowed_paths or getattr(view, 'get_view_name', lambda: None)() in allowed_names
  ```

  ```python
  # settings.py example:
  PUBLIC_API_PATHS = ['/api/health/', '/api/version/']
  PUBLIC_API_NAMES = ['API Docs', 'OpenAPI Schema']
  ```

* Default all other views to `IsAuthenticated` for production readiness.

* Apply Single Responsibility and Open/Closed principles by separating permission logic from view logic and centralizing it in permission classes.

* Register all models (`ImportSnapshot`, `Customer`, `Subscription`, `Resource`, `Meter`, `CostEntry`) with the Django Admin interface.

* Enable list display and filtering on relevant fields such as `snapshot_date`, `date`, `subscription`, `resource`.

* Use `list_filter`, `search_fields`, and `readonly_fields` where appropriate to aid data browsing.

* Staff-only upload via Django Admin.

* Management command for automated/scripted import.

* Default API security via DRF Token Authentication.

* Use a SOLID-compliant authentication strategy:

  * Implement a custom authentication class (`ConditionalTokenAuthentication`) that defers to settings to disable authentication in development mode via an `API_AUTH_DISABLED` flag.
  * Use a configuration provider (`EnvConfig`) to abstract away the environment logic, following Dependency Inversion.

* Allow selective public access to endpoints using DRF's `AllowAny` permission class on views that should be accessible without authentication.

  * Alternatively, create a `PublicEndpointPermission` class that checks `settings.PUBLIC_API_PATHS` or view names to grant access dynamically.

* Default all other views to `IsAuthenticated` for production readiness.

* Apply Single Responsibility and Open/Closed principles by separating permission logic from view logic and centralizing it in permission classes.

* Register all models (`ImportSnapshot`, `Customer`, `Subscription`, `Resource`, `Meter`, `CostEntry`) with the Django Admin interface.

* Enable list display and filtering on relevant fields such as `snapshot_date`, `date`, `subscription`, `resource`.

* Use `list_filter`, `search_fields`, and `readonly_fields` where appropriate to aid data browsing.

* Staff-only upload via Django Admin.

* Management command for automated/scripted import.

* Optional: DRF-based API for accessing cost data by date, snapshot, subscription, etc.

* Staff-only upload via Django Admin.

* Management command for automated/scripted import.

* Optional: DRF-based API for accessing cost data by date, snapshot, subscription, etc.

## API & Documentation

* Expose RESTful API using Django REST Framework (DRF):

  * Endpoints for listing, filtering, and aggregating `CostEntry`, `Subscription`, `ImportSnapshot`, etc.
  * Support query parameters for filtering by `date`, `snapshot_date`, `subscription_id`, etc.
* Enable pagination, ordering, and basic search.
* Add Swagger UI using `drf-spectacular`:

  * Configure `SPECTACULAR_SETTINGS` in settings.py.
  * Add `/api/schema/` (OpenAPI schema endpoint).
  * Add `/api/docs/` (Swagger UI endpoint).
* Versioning support (e.g., `/api/v1/cost-entries/`).

## Reporting & Analytics

* Daily and monthly cost aggregation.
* Change tracking between snapshots.
* Charts and export via Django views or BI tools.

## Future Extensions

* Azure Blob integration for auto-fetching files.
* Cost attribution by tag or project.
* Alerts and forecast logic.

---

This self-contained Django project provides a robust base for importing and analyzing Azure cost data over time, supporting future integration with APIs and dashboards.

