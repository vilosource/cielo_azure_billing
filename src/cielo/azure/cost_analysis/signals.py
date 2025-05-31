from django.db.models.signals import post_migrate
from django.dispatch import receiver
from django.core.management import call_command
import logging

logger = logging.getLogger(__name__)


@receiver(post_migrate)
def create_default_admin_signal(sender, **kwargs):
    """
    Automatically create default admin user after migrations are applied.
    This runs the create_admin management command when the billing app is migrated.
    """
    # Only run for our billing app migrations
    if sender.name == 'billing':
        try:
            call_command('create_admin')
        except Exception as e:
            logger.error(f"Failed to run create_admin command: {e}")
            print(f"❌ Failed to run create_admin command: {e}")
