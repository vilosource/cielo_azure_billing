from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model


class Command(BaseCommand):
    help = 'Create default admin user if no superuser exists'

    def handle(self, *args, **options):
        user_model = get_user_model()
        
        if not user_model.objects.filter(is_superuser=True).exists():
            user_model.objects.create_superuser(
                username='admin',
                email='admin@example.com',
                password='admin'
            )
            self.stdout.write(
                self.style.SUCCESS(
                    '✅ Default admin user created (username: admin, password: admin)'
                )
            )
        else:
            self.stdout.write(
                self.style.WARNING('⚠️  Superuser already exists, skipping admin creation')
            )
