import datetime
import calendar
from django.core.management.base import BaseCommand
from billing.models import BillingBlobSource, CostReportSnapshot, CostEntry


class Command(BaseCommand):
    help = 'Debug the AvailableReportDatesView to understand why it returns all dates in May'

    def add_arguments(self, parser):
        parser.add_argument(
            '--month',
            type=str,
            default='2025-05',
            help='Month to investigate (format: YYYY-MM)'
        )

    def handle(self, *args, **options):
        month_str = options['month']
        
        self.stdout.write(f"=== Debugging AvailableReportDatesView for {month_str} ===\n")
        
        # Parse month
        try:
            year, month = [int(part) for part in month_str.split('-')]
            month_date = datetime.date(year, month, 1)
        except ValueError:
            self.stderr.write(f"Invalid month format: {month_str}")
            return
            
        last_day = calendar.monthrange(month_date.year, month_date.month)[1]
        start_date = month_date
        end_date = month_date.replace(day=last_day)
        
        self.stdout.write(f"Date range: {start_date} to {end_date}")
        
        # Check database counts
        self.stdout.write("\n=== Database Overview ===")
        self.stdout.write(f"Total CostReportSnapshots: {CostReportSnapshot.objects.count()}")
        self.stdout.write(f"Total BillingBlobSources: {BillingBlobSource.objects.count()}")
        self.stdout.write(f"Active BillingBlobSources: {BillingBlobSource.objects.filter(is_active=True).count()}")
        self.stdout.write(f"Total CostEntries: {CostEntry.objects.count()}")
        
        # Check snapshots details
        self.stdout.write("\n=== CostReportSnapshot Details ===")
        for snapshot in CostReportSnapshot.objects.all():
            cost_entries_count = CostEntry.objects.filter(snapshot=snapshot).count()
            self.stdout.write(f"Snapshot {snapshot.id}: Status={snapshot.status}, "
                            f"Source={snapshot.source_id}, Created={snapshot.created_at}, "
                            f"CostEntries={cost_entries_count}")
            
            if cost_entries_count > 0:
                dates = CostEntry.objects.filter(snapshot=snapshot).values_list('date', flat=True).distinct().order_by('date')
                date_list = list(dates)
                if date_list:
                    self.stdout.write(f"  Date range: {min(date_list)} to {max(date_list)} ({len(date_list)} unique dates)")
                    if len(date_list) <= 10:
                        self.stdout.write(f"  All dates: {date_list}")
                    else:
                        self.stdout.write(f"  First 5 dates: {date_list[:5]}")
                        self.stdout.write(f"  Last 5 dates: {date_list[-5:]}")
        
        # Check active sources
        self.stdout.write("\n=== Active BillingBlobSource Details ===")
        for source in BillingBlobSource.objects.filter(is_active=True):
            self.stdout.write(f"Source {source.id}: Name={source.name}, Active={source.is_active}")
        
        # Simulate the AvailableReportDatesView logic
        self.stdout.write(f"\n=== Simulating AvailableReportDatesView Logic ===")
        
        snapshot_ids = []
        for source in BillingBlobSource.objects.filter(is_active=True):
            self.stdout.write(f"\nChecking source {source.id} ({source.name}):")
            
            # This is the exact query from AvailableReportDatesView
            snap = (
                CostReportSnapshot.objects.filter(
                    source=source,
                    status=CostReportSnapshot.Status.COMPLETE,
                    costentry__date__range=(start_date, end_date),
                )
                .order_by('-created_at')
                .first()
            )
            
            if snap:
                self.stdout.write(f"  Found snapshot: {snap.id} (created: {snap.created_at})")
                snapshot_ids.append(snap.id)
                
                # Check what cost entries this snapshot has in the date range
                cost_entries_in_range = CostEntry.objects.filter(
                    snapshot=snap,
                    date__range=(start_date, end_date)
                ).count()
                self.stdout.write(f"  CostEntries in date range: {cost_entries_in_range}")
                
            else:
                self.stdout.write("  No snapshot found for this source")
                
                # Let's see why - check if there are any snapshots for this source
                all_snapshots = CostReportSnapshot.objects.filter(source=source)
                self.stdout.write(f"  Total snapshots for this source: {all_snapshots.count()}")
                
                complete_snapshots = CostReportSnapshot.objects.filter(
                    source=source,
                    status=CostReportSnapshot.Status.COMPLETE
                )
                self.stdout.write(f"  Complete snapshots for this source: {complete_snapshots.count()}")
                
                snapshots_with_entries_in_range = CostReportSnapshot.objects.filter(
                    source=source,
                    status=CostReportSnapshot.Status.COMPLETE,
                    costentry__date__range=(start_date, end_date),
                )
                self.stdout.write(f"  Complete snapshots with entries in date range: {snapshots_with_entries_in_range.count()}")
        
        self.stdout.write(f"\nCollected snapshot IDs: {snapshot_ids}")
        
        if snapshot_ids:
            # Final query that gets the dates
            dates_query = CostEntry.objects.filter(
                snapshot_id__in=snapshot_ids, 
                date__range=(start_date, end_date)
            ).values_list('date', flat=True).distinct().order_by('date')
            
            dates = list(dates_query)
            self.stdout.write(f"\nFinal result - Available dates count: {len(dates)}")
            
            if len(dates) <= 10:
                self.stdout.write(f"All dates: {[d.isoformat() for d in dates]}")
            else:
                self.stdout.write(f"First 5 dates: {[d.isoformat() for d in dates[:5]]}")
                self.stdout.write(f"Last 5 dates: {[d.isoformat() for d in dates[-5:]]}")
                
            # Let's also check the SQL query being generated
            self.stdout.write(f"\nSQL Query: {dates_query.query}")
            
        else:
            self.stdout.write("\nNo snapshot IDs found - would return empty dates list")
        
        self.stdout.write("\n=== Investigation Complete ===")
