import datetime
import logging
from django.core.management.base import BaseCommand
from billing.models import BillingBlobSource

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Django management command to fetch Azure cost reports defined in
    BillingBlobSource and import them into the database.
    
    Uses SOLID design principles with service layer architecture to separate
    command-line processing from business logic.
    """
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
        """Entrypoint for command-line arguments and service invocation."""
        period = options.get("billing_period") or self._default_billing_period()
        names = options.get("only") or []
        dry_run = options.get("dry_run")
        overwrite = options.get("overwrite")
        
        # Create service instance and process sources
        service = ImportService(
            period=period,
            dry_run=dry_run,
            overwrite=overwrite,
            stdout=self.stdout,
            style=self.style
        )
        
        # Get sources to process
        sources = BillingBlobSource.objects.filter(is_active=True)
        if names:
            sources = sources.filter(name__in=names)
        
        if not sources:
            self.stdout.write(self.style.ERROR("No active billing sources found."))
            return
            
        # Print configuration and sources
        service.print_header(sources, names)
        
        # Process sources
        service.process_sources(sources)
        
        # Print summary report
        service.print_final_report()

    def _default_billing_period(self):
        """Generate default billing period as current month to current date."""
        today = datetime.date.today()
        start = today.replace(day=1)
        end = today
        return f"{start:%Y%m%d}-{end:%Y%m%d}"


class ImportService:
    """
    Service class for importing billing data from Azure sources.
    Handles the orchestration of processing multiple sources and runs,
    collecting statistics, and reporting results.
    """
    def __init__(self, period, dry_run, overwrite, stdout, style):
        self.period = period
        self.dry_run = dry_run
        self.overwrite = overwrite
        self.stdout = stdout
        self.style = style
        self.summary = []
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

    def print_header(self, sources, source_names):
        """Print formatted header for the import process with source details."""
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write(self.style.SUCCESS("Azure Billing Data Fetch and Import"))
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write("")
        
        # Configuration section
        self.stdout.write(self.style.SUCCESS("🔧 Configuration:"))
        self.stdout.write(f"  Billing Period: {self.period}")
        self.stdout.write(f"  Target Sources: {', '.join(source_names) if source_names else 'All active sources'}")
        self.stdout.write(f"  Dry Run: {'Yes' if self.dry_run else 'No'}")
        self.stdout.write(f"  Overwrite Existing: {'Yes' if self.overwrite else 'No'}")
        self.stdout.write("")
        
        # Sources section
        self.stdout.write(self.style.SUCCESS("📂 Sources to Process:"))
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write(f"{'Name':<20} | {'Status':<10} | {'Last Import':<20} | {'Base Folder'}")
        self.stdout.write("-" * 80)
        
        for source in sources:
            status = "✅ Active" if source.is_active else "❌ Inactive"
            last_import = source.last_imported_at or "Never"
            self.stdout.write(f"{source.name:<20} | {status:<10} | {last_import!s:<20} | {source.base_folder}")
        
        self.stdout.write(self.style.SUCCESS("=" * 80))
        self.stdout.write("")
        
        # Log start of processing
        logger.info(f"Starting fetch and import process for period: {self.period}")
        logger.info(f"Dry run mode: {self.dry_run}, Overwrite mode: {self.overwrite}")
        logger.info(f"Processing {sources.count()} active sources")

    def process_sources(self, sources):
        """Process all sources and collect statistics and results."""
        for source in sources:
            self.stdout.write("")
            self.stdout.write(self.style.SUCCESS(f"🔄 Processing source: {source.name}"))
            self.stdout.write(f"   Base folder: {source.base_folder}")
            
            # Create source processor and process this source
            processor = SourceProcessor(
                source=source,
                period=self.period,
                dry_run=self.dry_run,
                overwrite=self.overwrite,
                stdout=self.stdout,
                style=self.style,
                stats=self.stats,
                summary=self.summary
            )
            processor.process()

    def print_final_report(self):
        """Print comprehensive final report with statistics and detailed run info."""
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
        
        # Detailed run information
        if self.summary:
            self.stdout.write(self.style.SUCCESS("📋 Import Details:"))
            for item in self.summary:
                if item.get('run_id'):
                    self.stdout.write(f"  ✅ Run: {item['run_id']}")
                    self.stdout.write(f"    • Source: {item['source_name']}")
                    self.stdout.write(f"    • Status: {item['status']}")
                    if item.get('report_date'):
                        self.stdout.write(f"    • Report Date: {item['report_date']}")
                    if 'download_size' in item:
                        self.stdout.write(f"    • Size: {BillingBlobSource.format_bytes(item['download_size'])}")
                    if item.get('tmp_dir'):
                        self.stdout.write(f"    • Temp Dir: {item['tmp_dir']}")
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


class SourceProcessor:
    """
    Processes a single billing source, handling the fetch_and_import operation
    and recording statistics about the operation.
    """
    def __init__(self, source, period, dry_run, overwrite, stdout, style, stats, summary):
        self.source = source
        self.period = period
        self.dry_run = dry_run
        self.overwrite = overwrite
        self.stdout = stdout
        self.style = style
        self.stats = stats
        self.summary = summary

    def process(self):
        """Process this source and update statistics."""
        self.stats['sources_processed'] += 1
        logger.info(f"Starting processing for source: {self.source.name}")
        
        try:
            result = self.source.fetch_and_import(
                billing_period=self.period,
                dry_run=self.dry_run,
                overwrite=self.overwrite
            )
            
            self.stats['manifests_found'] += result['manifests_found']
            
            if result['status'] == 'no_manifests':
                self.stdout.write(self.style.WARNING("   ⚠️  No manifests found"))
                self._add_to_summary(None, 'no_manifests')
                return
            
            self._process_run_results(result['runs_processed'])
            
            self.stats['sources_successful'] += 1
            self.stdout.write(f"   ✅ Source completed: {len(result['runs_processed'])} runs processed")
            logger.info(f"Successfully completed source: {self.source.name}, processed {len(result['runs_processed'])} runs")
            
        except Exception as exc:
            self._handle_error(exc)

    def _process_run_results(self, runs_processed):
        """Process results from individual run imports."""
        for run_result in runs_processed:
            status = run_result['status']
            run_id = run_result.get('run_id')
            
            if status == 'skipped':
                self.stdout.write(f"     ⏭️  Skipping existing run: {run_id}")
                self.stats['runs_skipped'] += 1
            elif status == 'dry_run':
                self.stdout.write(f"     🏃 Dry run - files saved to: {run_result['tmp_dir']}")
                self.stats['runs_imported'] += 1
                self.stats['total_size_downloaded'] += run_result.get('download_size', 0)
            elif status == 'imported':
                self.stdout.write(f"     ✅ Import completed for run: {run_id}")
                self.stats['runs_imported'] += 1
                self.stats['total_size_downloaded'] += run_result.get('download_size', 0)
                
            # Add to summary for final report
            self._add_to_summary(run_result, status)
            
    def _add_to_summary(self, run_result, status):
        """Add processed run information to summary."""
        summary_item = {
            'source_name': self.source.name,
            'status': status
        }
        
        if run_result:
            summary_item['run_id'] = run_result.get('run_id')
            if 'tmp_dir' in run_result:
                summary_item['tmp_dir'] = run_result['tmp_dir']
            if 'download_size' in run_result:
                summary_item['download_size'] = run_result['download_size']
            if 'report_date' in run_result:
                summary_item['report_date'] = run_result['report_date']
            if 'reason' in run_result:
                summary_item['reason'] = run_result['reason']
                
        self.summary.append(summary_item)
            
    def _handle_error(self, exc):
        """Handle and record errors during source processing."""
        error_msg = f"Source {self.source.name}: {exc}"
        self.stats['errors'].append(error_msg)
        self.stats['sources_failed'] += 1
        
        logger.error("Failed to import from %s: %s", self.source.name, exc, exc_info=True)
        self.stdout.write(self.style.ERROR(f"   ❌ Failed: {exc}"))