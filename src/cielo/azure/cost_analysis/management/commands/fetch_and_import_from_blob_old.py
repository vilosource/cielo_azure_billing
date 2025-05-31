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
        self.stdout.write(f"  Total data downloaded: {self._format_bytes(self.stats['total_size_downloaded'])}")
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

    def _parse_base_folder(self, base):
        logger.debug(f"Parsing base folder: {base}")
        parsed = urlparse(base)
        logger.debug(f"Parsed URL - scheme: {parsed.scheme}, netloc: {parsed.netloc}, path: {parsed.path}")
        
        path = parsed.path.lstrip("/")
        parts = path.split("/", 1)
        container = parts[0]
        prefix = ""
        if len(parts) > 1:
            prefix = parts[1].rstrip("/") + "/"
        container_url = f"{parsed.scheme}://{parsed.netloc}/{container}"
        
        logger.debug(f"Extracted container: {container}, prefix: {prefix}, container_url: {container_url}")
        return container_url, prefix

    def _process_source(self, source, period, dry_run, overwrite):
        self.stats['sources_processed'] += 1
        source.last_attempted_at = datetime.datetime.now(datetime.timezone.utc)
        
        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS(f"🔄 Processing source: {source.name}"))
        self.stdout.write(f"   Base folder: {source.base_folder}")
        logger.info(f"Starting processing for source: {source.name}")
        
        try:
            if DefaultAzureCredential is None:
                raise RuntimeError("Azure SDK not installed")

            logger.debug("Initializing Azure credentials...")
            cred = DefaultAzureCredential()
            logger.debug("Azure credentials initialized successfully")
            
            container_url, prefix = self._parse_base_folder(source.base_folder)
            logger.debug(f"Parsed container URL: {container_url}, prefix: {prefix}")
            
            client = ContainerClient.from_container_url(container_url, credential=cred)

            listing_prefix = prefix
            if period:
                listing_prefix = f"{prefix}{period}/"

            logger.info(f"Listing blobs with prefix: {listing_prefix}")
            self.stdout.write(f"   🔍 Searching blobs with prefix: {listing_prefix}")
            
            blobs = client.list_blobs(name_starts_with=listing_prefix)
            blob_list = list(blobs)

            logger.info(f"Found {len(blob_list)} blobs in container {container_url} with prefix {listing_prefix}")
            self.stdout.write(f"   📂 Found {len(blob_list)} blobs")
            
            # If no blobs found with period-specific prefix, try searching without period
            if not blob_list and period:
                logger.info(f"No blobs found with period prefix, trying base prefix: {prefix}")
                self.stdout.write(f"   🔍 No files found with period prefix, searching base path: {prefix}")
                
                blobs = client.list_blobs(name_starts_with=prefix)
                blob_list = list(blobs)
                logger.info(f"Found {len(blob_list)} blobs in container {container_url} with base prefix {prefix}")
                self.stdout.write(f"   📂 Found {len(blob_list)} blobs in base path")
                
                if blob_list:
                    self.stdout.write(f"   💡 Tip: Files may not be organized by billing period. Consider running without --billing-period")
            
            manifests = [b for b in blob_list if b.name.endswith("manifest.json")]
            
            logger.info(f"Found {len(blob_list)} total blobs, {len(manifests)} manifests")
            self.stdout.write(f"   📄 Found {len(blob_list)} blobs, {len(manifests)} manifests")
            
            self.stats['manifests_found'] += len(manifests)
            
            if not manifests:
                source.status = "no-manifests"
                source.save(update_fields=["last_attempted_at", "status"])
                logger.warning("No manifests found for %s", source.name)
                self.stdout.write(self.style.WARNING(f"   ⚠️  No manifests found"))
                return

            runs_processed = 0
            for i, blob in enumerate(manifests, 1):
                self.stdout.write(f"   📋 Processing manifest {i}/{len(manifests)}: {blob.name}")
                logger.info(f"Processing manifest {i}/{len(manifests)}: {blob.name}")
                
                manifest_url = f"{container_url}/{blob.name}"
                logger.debug(f"Downloading manifest from: {manifest_url}")
                
                bclient = BlobClient.from_blob_url(manifest_url, credential=cred)
                manifest_data = json.loads(bclient.download_blob().readall())
                
                run_info = manifest_data.get("runInfo", {})
                run_id = run_info.get("runId")
                report_date_raw = run_info.get("endDate")
                
                logger.debug(f"Extracted run_id: {run_id}, end_date: {report_date_raw}")
                
                report_date = None
                if report_date_raw:
                    report_date = datetime.date.fromisoformat(
                        report_date_raw.split("T")[0]
                    )
                
                if (
                    CostReportSnapshot.objects.filter(run_id=run_id).exists()
                    and not overwrite
                ):
                    logger.info("Skip existing run %s for %s", run_id, source.name)
                    self.stdout.write(f"     ⏭️  Skipping existing run: {run_id}")
                    self.stats['runs_skipped'] += 1
                    continue

                blob_name = manifest_data.get("blobs", [{}])[0].get("blobName")
                if not blob_name:
                    raise RuntimeError("No blobName in manifest")
                
                csv_url = f"{container_url}/{blob_name}"
                logger.debug(f"Downloading CSV from: {csv_url}")
                self.stdout.write(f"     📥 Downloading: {blob_name}")
                
                bclient = BlobClient.from_blob_url(csv_url, credential=cred)
                csv_blob_data = bclient.download_blob().readall()
                
                # Track download size
                download_size = len(csv_blob_data)
                self.stats['total_size_downloaded'] += download_size
                logger.debug(f"Downloaded {download_size} bytes")
                self.stdout.write(f"     💾 Downloaded {self._format_bytes(download_size)}")

                tmp_dir = Path(tempfile.mkdtemp())
                gz_path = tmp_dir / Path(blob_name).name
                manifest_path = tmp_dir / "manifest.json"
                
                logger.debug(f"Saving files to temporary directory: {tmp_dir}")
                
                with open(gz_path, "wb") as fh:
                    fh.write(csv_blob_data)
                with open(manifest_path, "w", encoding="utf-8") as fh:
                    json.dump(manifest_data, fh)
                    
                import gzip

                csv_path = tmp_dir / (gz_path.stem)
                logger.debug(f"Decompressing {gz_path} to {csv_path}")
                self.stdout.write(f"     📦 Decompressing data...")
                
                with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
                    f_out.write(f_in.read())
                    
                if dry_run:
                    source.status = "dry-run"
                    logger.info(
                        "Dry run completed for %s. Files stored at %s",
                        source.name,
                        tmp_dir,
                    )
                    self.stdout.write(f"     🏃 Dry run - files saved to: {tmp_dir}")
                else:
                    logger.info(f"Starting import for run_id: {run_id}")
                    self.stdout.write(f"     📊 Importing data for run: {run_id}")
                    
                    importer = CostCsvImporter(
                        str(csv_path),
                        run_id=run_id,
                        report_date=report_date,
                        source=source,
                    )
                    importer.import_file()
                    
                    source.last_imported_at = datetime.datetime.now(datetime.timezone.utc)
                    source.status = "imported"
                    
                    logger.info(f"Successfully imported run_id: {run_id}")
                    self.stdout.write(f"     ✅ Import completed successfully")
                    
                self.stats['runs_imported'] += 1
                runs_processed += 1
                
            source.save(
                update_fields=["last_attempted_at", "last_imported_at", "status"]
            )
            
            self.stats['sources_successful'] += 1
            self.stdout.write(f"   ✅ Source completed: {runs_processed} runs processed")
            logger.info(f"Successfully completed source: {source.name}, processed {runs_processed} runs")
            
        except Exception as exc:
            source.status = f"error: {exc}"
            source.save(update_fields=["last_attempted_at", "status"])
            error_msg = f"Source {source.name}: {exc}"
            self.stats['errors'].append(error_msg)
            self.stats['sources_failed'] += 1
            
            logger.error("Failed to import from %s: %s", source.name, exc, exc_info=True)
            self.stdout.write(self.style.ERROR(f"   ❌ Failed: {exc}"))

    def _format_bytes(self, size_bytes):
        """Convert bytes to human readable format"""
        if size_bytes == 0:
            return "0 B"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"
