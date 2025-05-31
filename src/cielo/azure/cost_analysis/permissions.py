from rest_framework.permissions import BasePermission, SAFE_METHODS
from django.conf import settings


class PublicEndpointPermission(BasePermission):
    def has_permission(self, request, view):
        if getattr(settings, 'API_AUTH_DISABLED', False):
            return True
        allowed_paths = getattr(settings, 'PUBLIC_API_PATHS', [])
        if request.path in allowed_paths:
            return True
        allowed_names = getattr(settings, 'PUBLIC_API_NAMES', [])
        if hasattr(view, 'get_view_name') and view.get_view_name() in allowed_names:
            return True
        return request.method in SAFE_METHODS
