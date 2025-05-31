# Caching in `cielo.azure.cost_analysis`

The cost analysis app includes a lightweight caching helper located at
`cielo.azure.cost_analysis.caching`. It provides a `get_cache_backend()`
function which returns a cache interface used by the summary API views.

## Where it is used

`get_cache_backend()` is imported in `cielo.azure.cost_analysis.views` and
leveraged by the summary endpoints to store aggregated results. This prevents
expensive database queries on repeated requests. See `views.py` for the
implementation details.

## Selecting a backend

The helper checks the Django setting `COST_CACHE_IMPLEMENTATION` to decide which
backend to use. Two options are provided:

- `"memory"` – uses Django's default in-memory cache. This is enabled when
  `DEBUG` is `True`.
- `"redis"` – uses the configured Redis cache connection. This is enabled by
default when `DEBUG` is `False`.

You can override `COST_CACHE_IMPLEMENTATION` in your project settings to choose
the backend that fits your environment.

