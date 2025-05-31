# Integrating `cielo.azure.cost_analysis`

This guide explains how to set up a new Django project that uses the
`cielo.azure.cost_analysis` application or how to integrate it into an
existing project.

## Installation

1. Install the package from your local checkout or wheel:
   ```bash
   pip install -e /path/to/cielo_azure_billing
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

## Using the Application

The app exposes a REST API for cost data. After running the server you can visit
`/api/docs/` for interactive API documentation. See `README.md` for more
information about available endpoints and commands.

## Existing Projects

If you are adding the cost analysis app to an existing Django project, make sure
any custom authentication or caching settings are compatible with the defaults
provided in `cielo.azure.cost_analysis`. Review the example `settings.py` in this
repository for guidance on logging and middleware configuration.
