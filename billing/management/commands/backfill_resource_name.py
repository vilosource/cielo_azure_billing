from django.core.management.base import BaseCommand
from billing.models import Resource


class Command(BaseCommand):
    help = 'Backfill resource_name from resource_id for existing resources'

    def handle(self, *args, **options):
        updated = 0
        for resource in Resource.objects.all():
            if not resource.resource_name:
                resource.resource_name = resource.resource_id.rstrip('/')\
                    .split('/')[-1]
                resource.save(update_fields=['resource_name'])
                updated += 1

        self.stdout.write(self.style.SUCCESS(f'Backfilled {updated} resources'))
