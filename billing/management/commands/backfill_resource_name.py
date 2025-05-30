from django.core.management.base import BaseCommand
from billing.models import Resource


class Command(BaseCommand):
    help = 'Backfill resource_name from resource_id for existing resources'

    def handle(self, *args, **options):
        updated = 0
        for resource in Resource.objects.all():
            print(f'Processing resource: {resource.resource_id}')
            if not resource.resource_group
                resource.resource_group = resource.resource_group.rstrip('/')\
                    .split('/')[-1]
                print(f'Updating resource_name for {resource.resource_id} to {resource.resource_group}')
                #resource.save(update_fields=['resource_name'])
                updated += 1

        self.stdout.write(self.style.SUCCESS(f'Backfilled {updated} resources'))
