import datetime
import logging
from django.core.management.base import BaseCommand
from billing.models import BillingBlobSource

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Fetch Azure cost reports defined in BillingBlobSource and import them"

    def add_arguments(self, parser):
        parser.add_argument(
            "--billing-period",
            help="Billing period in YYYYMMDD-YYYYMMDD format",
        )
        parser.add_argument(
            "--only",
            action="append",
            help="Process only sources matching this name",
        )
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--overwrite", action="store_true")

    def handle(self, *args, **options):
        period = options.get("billing_period") or self._default_billing_period()
        names = options.get("only") or []
        dry_run = options.get("dry_run")
        overwrite = options.get("overwrite")
        
        # Print header
        self._print_header(period, names, dry_run, overwrite)
        
        sources = BillingBlobSource.objects.filter(is_active=True)
        if names:
            sources = sources.filter(name__in=names)
            
        logger.info(f"Starting fetch and import process for period: {period}")
        logger.info(f"Dry run mode: {dry_run}, Overwrite mode: {overwrite}")
        logger.info(f"Processing {sources.count()} active sources")
        
        # Track overall statistics
        self.stats = {
            'sources_processed': 0,
            'sources_successful': 0,
            'sources_failed': 0,
            'manifests_found': 0,
            'runs_imported': 0,
            'runs_skipped': 0,
            'total_size_downloaded': 0,
            'errors': []
        }
        
        for source in sources:
            self._process_source(source, period, dry_run, overwrite)
            
        self._print_final_report()

    def _print_header(self, period, names, dry_run, overwrite):
        """Print formatted header for the import process"""
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(self.style.SUCCESS("Azure Billing Data Fetch and Import"))
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("🔧 Configuration:"))
        self.stdout.write(f"  Billing Period: {period}")
        self.stdout.write(f"  Target Sources: {', '.join(names) if names else 'All active sources'}")
        self.stdout.write(f"  Dry Run: {'Yes' if dry_run else 'No'}")
        self.stdout.write(f"  Overwrite Existing: {'Yes' if overwrite else 'No'}")
        self.stdout.write("")

    def _print_final_report(self):
        """Print comprehensive final report"""
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(self.style.SUCCESS("📊 Final Import Report"))
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write("")
        
        # Summary statistics
        self.stdout.write(self.style.SUCCESS("📈 Summary Statistics:"))
        self.stdout.write(f"  Sources processed: {self.stats['sources_processed']}")
        self.stdout.write(f"  Successful imports: {self.stats['sources_successful']}")
        self.stdout.write(f"  Failed imports: {self.stats['sources_failed']}")
        self.stdout.write(f"  Manifests found: {self.stats['manifests_found']}")
        self.stdout.write(f"  Runs imported: {self.stats['runs_imported']}")
        self.stdout.write(f"  Runs skipped: {self.stats['runs_skipped']}")
        self.stdout.write(f"  Total data downloaded: {BillingBlobSource.format_bytes(self.stats['total_size_downloaded'])}")
        self.stdout.write("")
        
        # Status summary
        if self.stats['sources_failed'] == 0:
            self.stdout.write(self.style.SUCCESS("✅ All sources processed successfully!"))
        else:
            self.stdout.write(self.style.WARNING(f"⚠️  {self.stats['sources_failed']} sources had errors"))
            
        # Error details
        if self.stats['errors']:
            self.stdout.write("")
            self.stdout.write(self.style.ERROR("❌ Errors encountered:"))
            for error in self.stats['errors']:
                self.stdout.write(f"  • {error}")
                
        self.stdout.write("")
        logger.info("Import process completed. Final stats: %s", self.stats)

    def _default_billing_period(self):
        today = datetime.date.today()
        start = today.replace(day=1)
        end = today
        return f"{start:%Y%m%d}-{end:%Y%m%d}"

    def _process_source(self, source, period, dry_run, overwrite):
        self.stats['sources_processed'] += 1
        
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"🔄 Processing source: {source.name}"))
        self.stdout.write(f"   Base folder: {source.base_folder}")
        logger.info(f"Starting processing for source: {source.name}")
        
        try:
            # Use the new model method
            result = source.fetch_and_import(
                billing_period=period,
                dry_run=dry_run,
                overwrite=overwrite
            )
            
            self.stats['manifests_found'] += result['manifests_found']
            
            if result['status'] == 'no_manifests':
                self.stdout.write(self.style.WARNING("   ⚠️  No manifests found"))
                return
            
            # Process run results
            for run_result in result['runs_processed']:
                if run_result['status'] == 'skipped':
                    self.stdout.write(f"     ⏭️  Skipping existing run: {run_result['run_id']}")
                    self.stats['runs_skipped'] += 1
                elif run_result['status'] == 'dry_run':
                    self.stdout.write(f"     🏃 Dry run - files saved to: {run_result['tmp_dir']}")
                    self.stats['runs_imported'] += 1
                    self.stats['total_size_downloaded'] += run_result['download_size']
                elif run_result['status'] == 'imported':
                    self.stdout.write(f"     ✅ Import completed for run: {run_result['run_id']}")
                    self.stats['runs_imported'] += 1
                    self.stats['total_size_downloaded'] += run_result['download_size']
            
            self.stats['sources_successful'] += 1
            self.stdout.write(f"   ✅ Source completed: {len(result['runs_processed'])} runs processed")
            logger.info(f"Successfully completed source: {source.name}, processed {len(result['runs_processed'])} runs")
            
        except Exception as exc:
            error_msg = f"Source {source.name}: {exc}"
            self.stats['errors'].append(error_msg)
            self.stats['sources_failed'] += 1
            
            logger.error("Failed to import from %s: %s", source.name, exc, exc_info=True)
            self.stdout.write(self.style.ERROR(f"   ❌ Failed: {exc}"))
