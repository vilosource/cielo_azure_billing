from django.db import models
import datetime
import json
import logging
import tempfile
from pathlib import Path
from urllib.parse import urlparse

try:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobClient, ContainerClient
except Exception:  # pragma: no cover - libs optional for offline env
    DefaultAzureCredential = None
    BlobClient = None
    ContainerClient = None

logger = logging.getLogger(__name__)


class BillingBlobSource(models.Model):
    """Configuration for locating cost export blobs."""

    name = models.CharField(max_length=100)
    base_folder = models.CharField(
        max_length=255,
        help_text="Base export folder e.g. costreports/prod/prod-actual-cost/",
    )
    is_active = models.BooleanField(default=True)
    last_imported_at = models.DateTimeField(null=True, blank=True)
    last_attempted_at = models.DateTimeField(null=True, blank=True)
    status = models.CharField(max_length=64, null=True, blank=True)

    class Meta:
        ordering = ["name"]

    def __str__(self):
        return self.name

    @staticmethod
    def parse_base_folder(base):
        """Parse base folder URL into container URL and prefix."""
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

    def get_azure_client(self):
        """Get Azure container client for this blob source."""
        if DefaultAzureCredential is None:
            raise RuntimeError("Azure SDK not installed")

        cred = DefaultAzureCredential()
        container_url, prefix = self.parse_base_folder(self.base_folder)
        client = ContainerClient.from_container_url(container_url, credential=cred)
        return client, container_url, prefix

    def list_blobs(self, billing_period=None):
        """List all blobs for this source, optionally filtered by billing period."""
        client, container_url, prefix = self.get_azure_client()

        listing_prefix = prefix
        if billing_period:
            listing_prefix = f"{prefix}{billing_period}/"

        logger.info(f"Listing blobs with prefix: {listing_prefix}")
        blobs = client.list_blobs(name_starts_with=listing_prefix)
        blob_list = list(blobs)

        logger.info(f"Found {len(blob_list)} blobs in container {container_url} with prefix {listing_prefix}")

        # If no blobs found with period-specific prefix, try searching without period
        if not blob_list and billing_period:
            logger.info(f"No blobs found with period prefix, trying base prefix: {prefix}")
            blobs = client.list_blobs(name_starts_with=prefix)
            blob_list = list(blobs)
            logger.info(f"Found {len(blob_list)} blobs in container {container_url} with base prefix {prefix}")

        return blob_list, container_url

    def get_manifests(self, billing_period=None):
        """Get all manifest files for this source."""
        blob_list, container_url = self.list_blobs(billing_period)
        manifests = [b for b in blob_list if b.name.endswith("manifest.json")]
        logger.info(f"Found {len(blob_list)} total blobs, {len(manifests)} manifests")
        return manifests, container_url

    def get_manifest_data(self, manifest_blob, container_url):
        """Download and parse manifest data."""
        manifest_url = f"{container_url}/{manifest_blob.name}"
        logger.debug(f"Downloading manifest from: {manifest_url}")

        cred = DefaultAzureCredential()
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

        return {
            'manifest_data': manifest_data,
            'run_id': run_id,
            'report_date': report_date
        }

    def download_csv_blob(self, manifest_data, container_url):
        """Download CSV blob from manifest data."""
        blob_name = manifest_data.get("blobs", [{}])[0].get("blobName")
        if not blob_name:
            raise RuntimeError("No blobName in manifest")

        csv_url = f"{container_url}/{blob_name}"
        logger.debug(f"Downloading CSV from: {csv_url}")

        cred = DefaultAzureCredential()
        bclient = BlobClient.from_blob_url(csv_url, credential=cred)
        csv_blob_data = bclient.download_blob().readall()

        return csv_blob_data, blob_name

    def process_import_run(self, manifest_blob, container_url, dry_run=False, overwrite=False):
        """Process a single import run from a manifest blob."""
        from billing.services import CostCsvImporter
        
        # Get manifest data
        manifest_info = self.get_manifest_data(manifest_blob, container_url)
        manifest_data = manifest_info['manifest_data']
        run_id = manifest_info['run_id']
        report_date = manifest_info['report_date']
        
        # Check if already imported - use string reference to avoid circular import
        from django.apps import apps
        CostReportSnapshot = apps.get_model('billing', 'CostReportSnapshot')
        if CostReportSnapshot.objects.filter(run_id=run_id).exists() and not overwrite:
            logger.info("Skip existing run %s for %s", run_id, self.name)
            return {'status': 'skipped', 'run_id': run_id, 'reason': 'already_exists'}

        # Download CSV data
        csv_blob_data, blob_name = self.download_csv_blob(manifest_data, container_url)
        download_size = len(csv_blob_data)
        logger.debug(f"Downloaded {download_size} bytes")

        # Save to temporary files
        tmp_dir = Path(tempfile.mkdtemp())
        gz_path = tmp_dir / Path(blob_name).name
        manifest_path = tmp_dir / "manifest.json"

        logger.debug(f"Saving files to temporary directory: {tmp_dir}")

        with open(gz_path, "wb") as fh:
            fh.write(csv_blob_data)
        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest_data, fh)

        # Decompress if needed
        import gzip
        csv_path = tmp_dir / (gz_path.stem)
        logger.debug(f"Decompressing {gz_path} to {csv_path}")

        with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
            f_out.write(f_in.read())

        if dry_run:
            self.status = "dry-run"
            logger.info("Dry run completed for %s. Files stored at %s", self.name, tmp_dir)
            return {
                'status': 'dry_run',
                'run_id': run_id,
                'tmp_dir': str(tmp_dir),
                'download_size': download_size
            }
        else:
            logger.info(f"Starting import for run_id: {run_id}")

            importer = CostCsvImporter(
                str(csv_path),
                run_id=run_id,
                report_date=report_date,
                source=self,
            )
            importer.import_file()

            self.last_imported_at = datetime.datetime.now(datetime.timezone.utc)
            self.status = "imported"

            logger.info(f"Successfully imported run_id: {run_id}")
            return {
                'status': 'imported',
                'run_id': run_id,
                'download_size': download_size
            }

    def fetch_and_import(self, billing_period=None, dry_run=False, overwrite=False):
        """Fetch and import all available runs for this source."""
        self.last_attempted_at = datetime.datetime.now(datetime.timezone.utc)

        try:
            manifests, container_url = self.get_manifests(billing_period)

            if not manifests:
                self.status = "no-manifests"
                self.save(update_fields=["last_attempted_at", "status"])
                return {'status': 'no_manifests', 'manifests_found': 0, 'runs_processed': []}

            runs_processed = []
            for manifest_blob in manifests:
                result = self.process_import_run(manifest_blob, container_url, dry_run, overwrite)
                runs_processed.append(result)

            if not dry_run:
                self.save(update_fields=["last_attempted_at", "last_imported_at", "status"])

            return {
                'status': 'success',
                'manifests_found': len(manifests),
                'runs_processed': runs_processed
            }

        except Exception as exc:
            self.status = f"error: {exc}"
            self.save(update_fields=["last_attempted_at", "status"])
            raise

    def inspect_available_runs(self, billing_period=None):
        """Inspect available export runs without importing."""
        blob_list, container_url = self.list_blobs(billing_period)
        
        manifests = [b for b in blob_list if b.name.endswith("manifest.json")]
        csv_files = [b for b in blob_list if b.name.endswith((".csv", ".csv.gz"))]
        other_files = [b for b in blob_list if not b.name.endswith("manifest.json") and not b.name.endswith((".csv", ".csv.gz"))]
        
        runs_data = []
        for manifest_blob in manifests:
            manifest_info = self.get_manifest_data(manifest_blob, container_url)
            run_id = manifest_info['run_id']
            
            # Use string reference to avoid circular import
            from django.apps import apps
            CostReportSnapshot = apps.get_model('billing', 'CostReportSnapshot')
            imported = CostReportSnapshot.objects.filter(run_id=run_id).exists()

            run_data = {
                'run_id': run_id,
                'end_date': manifest_info['report_date'],
                'size': manifest_blob.size,
                'imported': imported,
                'blob_name': manifest_blob.name,
                'last_modified': manifest_blob.last_modified
            }
            runs_data.append(run_data)

        return {
            'total_blobs': len(blob_list),
            'manifests': len(manifests),
            'csv_files': len(csv_files),
            'other_files': len(other_files),
            'runs_data': runs_data,
            'blob_details': {
                'manifests': manifests,
                'csv_files': csv_files,
                'other_files': other_files
            }
        }

    @staticmethod
    def format_bytes(size_bytes):
        """Convert bytes to human readable format."""
        if size_bytes == 0:
            return "0 B"

        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"


class CostReportSnapshotQuerySet(models.QuerySet):
    def latest_per_subscription(self):
        from django.db.models import Max

        latest = (
            CostEntry.objects.filter(
                snapshot__status=CostReportSnapshot.Status.COMPLETE
            )
            .values("subscription_id")
            .annotate(latest_id=Max("snapshot_id"))
        )
        return self.filter(id__in=[item["latest_id"] for item in latest])

    def latest_overall(self):
        return self.order_by("-created_at").first()

    def for_day(self, target_date):
        return self.filter(
            report_date=target_date,
            status=CostReportSnapshot.Status.COMPLETE,
        )


class CostReportSnapshot(models.Model):
    class Status(models.TextChoices):
        IN_PROGRESS = "in_progress", "In Progress"
        COMPLETE = "complete", "Complete"
        FAILED = "failed", "Failed"

    run_id = models.CharField(max_length=64, unique=True, db_index=True)
    report_date = models.DateField(null=True, blank=True)
    file_name = models.CharField(max_length=255)
    source = models.ForeignKey(
        "BillingBlobSource", null=True, on_delete=models.SET_NULL
    )
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.IN_PROGRESS,
        db_index=True,
    )
    created_at = models.DateTimeField(auto_now_add=True)

    objects = CostReportSnapshotQuerySet.as_manager()

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.file_name} @ {self.report_date or self.created_at.date()}"


class Customer(models.Model):
    tenant_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return self.name or self.tenant_id


class Subscription(models.Model):
    subscription_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=255)
    customer = models.ForeignKey(Customer, on_delete=models.CASCADE)

    def __str__(self):
        return self.name


class Resource(models.Model):
    resource_id = models.TextField(unique=True)
    resource_name = models.CharField(max_length=255, null=True, blank=True)
    name = models.CharField(max_length=255, null=True, blank=True)
    resource_group = models.CharField(max_length=255, null=True, blank=True)
    location = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return self.resource_name


class Meter(models.Model):
    meter_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=255)
    category = models.CharField(max_length=255)
    subcategory = models.CharField(max_length=255, null=True, blank=True)
    service_family = models.CharField(max_length=255, null=True, blank=True)
    unit = models.CharField(max_length=64)

    def __str__(self):
        return self.name


class CostEntryQuerySet(models.QuerySet):
    """Queryset utilities for :class:`CostEntry`."""

    def latest_for_day(self, target_date):
        """Return entries from the latest snapshot per subscription for ``target_date``."""
        from django.db.models import Max
        logger.debug(f"Fetching latest cost entries for date: {target_date}")
        latest_ids = (
            self.model.objects.filter(
                date=target_date,
                snapshot__status=CostReportSnapshot.Status.COMPLETE,
            )
            .values("subscription_id")
            .annotate(latest_id=Max("snapshot_id"))
            .values_list("latest_id", flat=True)
        )

        return self.filter(date=target_date, snapshot_id__in=list(latest_ids))


class CostEntry(models.Model):
    snapshot = models.ForeignKey(CostReportSnapshot, on_delete=models.CASCADE)
    date = models.DateField(db_index=True)
    subscription = models.ForeignKey(Subscription, on_delete=models.CASCADE)
    resource = models.ForeignKey(Resource, on_delete=models.CASCADE)
    meter = models.ForeignKey(Meter, on_delete=models.CASCADE)
    cost_in_usd = models.DecimalField(max_digits=12, decimal_places=4)
    cost_in_billing_currency = models.DecimalField(
        max_digits=12, decimal_places=4, null=True, blank=True
    )
    billing_currency = models.CharField(max_length=10, null=True, blank=True)
    quantity = models.DecimalField(max_digits=12, decimal_places=4)
    unit_price = models.DecimalField(max_digits=12, decimal_places=6)
    payg_price = models.DecimalField(
        max_digits=12, decimal_places=6, null=True, blank=True
    )
    pricing_model = models.CharField(max_length=64, null=True, blank=True)
    charge_type = models.CharField(max_length=64, null=True, blank=True)
    publisher_name = models.CharField(max_length=255, null=True, blank=True)
    cost_center = models.CharField(max_length=255, null=True, blank=True)
    tags = models.JSONField(null=True, blank=True)

    objects = CostEntryQuerySet.as_manager()

    def __str__(self):
        return f"{self.date} - {self.subscription}"

    class Meta:
        unique_together = (
            "snapshot",
            "date",
            "subscription",
            "resource",
            "meter",
            "quantity",
            "unit_price",
        )
