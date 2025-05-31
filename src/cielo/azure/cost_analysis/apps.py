from django.apps import AppConfig


class CostAnalysisConfig(AppConfig):
    """Application configuration for the cost analysis app."""

    default_auto_field = 'django.db.models.BigAutoField'
    name = 'cielo.azure.cost_analysis'

    def ready(self):
        from . import signals  # Import signals to register them
