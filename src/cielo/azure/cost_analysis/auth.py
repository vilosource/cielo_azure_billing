from rest_framework.authentication import TokenAuthentication
from django.conf import settings


class ConditionalTokenAuthentication(TokenAuthentication):
    def authenticate(self, request):
        if getattr(settings, 'API_AUTH_DISABLED', False):
            return None
        return super().authenticate(request)
