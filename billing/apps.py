from django.apps import AppConfig
from django.db.utils import OperationalError, ProgrammingError
from django.contrib.auth import get_user_model


def create_default_admin():
    user_model = get_user_model()
    if not user_model.objects.filter(is_superuser=True).exists():
        user_model.objects.create_superuser('admin', 'admin@example.com', 'admin')


class BillingConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'billing'

    def ready(self):
        try:
            create_default_admin()
        except (OperationalError, ProgrammingError):
            # Database might not be ready yet (e.g., during migrate)
            pass
