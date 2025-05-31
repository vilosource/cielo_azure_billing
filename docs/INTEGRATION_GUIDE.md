# Integrating `cielo.azure.cost_analysis`

This guide explains how to set up a new Django project that uses the
`cielo.azure.cost_analysis` application or how to integrate it into an
existing project.

## Installation

1. Install the package from your local checkout or wheel:
   ```bash
   pip install -e /path/to/example_django_project
   ```
   or if published to PyPI:
   ```bash
   pip install cielo.azure.cost_analysis
   ```

2. Add `cielo.azure.cost_analysis` to `INSTALLED_APPS` in your Django project's
   `settings.py`:
   ```python
   INSTALLED_APPS = [
       # ...
       'cielo.azure.cost_analysis',
   ]
   ```

3. Include the app's URLs in your root `urls.py`:
   ```python
   from django.urls import include, path

   urlpatterns = [
       # ...
       path('api/', include('cielo.azure.cost_analysis.urls')),
   ]
   ```

4. Run the migrations to create the database tables:
   ```bash
   python manage.py migrate
   ```

## Project Layout

The repository includes a minimal example project named
`example_django_project`. It uses a local SQLite database and enables caching
through the `COST_CACHE_IMPLEMENTATION` setting. Use this project as a template
when integrating the app into your own codebase.

## Using the Application

The app exposes a REST API for cost data. After running the server you can visit
`/api/docs/` for interactive API documentation. See `README.md` for more
information about available endpoints and commands.

## Existing Projects

If you are adding the cost analysis app to an existing Django project, make sure
any custom authentication or caching settings are compatible with the defaults
provided in `cielo.azure.cost_analysis`. Review the example `settings.py` in this
repository for guidance on logging and middleware configuration.

## Caching

Summary API endpoints store results using the helper in
`cielo.azure.cost_analysis.caching`. Configure
`COST_CACHE_IMPLEMENTATION` in your settings to `"memory"` or `"redis"` to select
the backend.

## Management Commands

Use the provided management commands to import cost data and maintain resource
information:

```bash
python manage.py import_cost_csv --file path/to/report.csv
python manage.py backfill_resource_name
python manage.py fetch_and_import_from_blob
```

Schedule these commands as needed to keep your cost database up to date.
