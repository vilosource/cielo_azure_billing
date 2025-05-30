class BaseCostCache:
    """Interface for cache backends used by cost summary views."""
    def get(self, key):
        raise NotImplementedError

    def set(self, key, value, timeout=None):
        raise NotImplementedError


class MemoryCostCache(BaseCostCache):
    """In-memory cache using Django's default cache."""
    def __init__(self, cache=None):
        from django.core.cache import cache as default_cache
        self._cache = cache or default_cache

    def get(self, key):
        return self._cache.get(key)

    def set(self, key, value, timeout=None):
        self._cache.set(key, value, timeout=timeout)


class RedisCostCache(MemoryCostCache):
    """Redis cache backend. Uses the configured default cache instance."""
    pass


def get_cache_backend():
    """Return cache backend instance based on settings.COST_CACHE_IMPLEMENTATION."""
    from django.conf import settings

    impl = getattr(settings, "COST_CACHE_IMPLEMENTATION", "memory")
    if impl == "redis":
        return RedisCostCache()
    return MemoryCostCache()
