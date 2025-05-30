import datetime
import json
import logging
from pathlib import Path
from django.core.management.base import BaseCommand
from billing.models import BillingBlobSource, ImportSnapshot
from billing.services import CostCsvImporter

try:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobClient
except Exception:  # pragma: no cover - libs optional for offline env
    DefaultAzureCredential = None
    BlobClient = None

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
        sources = BillingBlobSource.objects.filter(is_active=True)
        if names:
            sources = sources.filter(name__in=names)
        for source in sources:
            self._process_source(source, period, dry_run, overwrite)

    def _default_billing_period(self):
        today = datetime.date.today()
        start = today.replace(day=1)
        end = today
        return f"{start:%Y%m%d}-{end:%Y%m%d}"

    def _process_source(self, source, period, dry_run, overwrite):
        source.last_attempted_at = datetime.datetime.utcnow()
        try:
            if DefaultAzureCredential is None:
                raise RuntimeError("Azure SDK not installed")
            manifest_url = source.path_template.format(billing_period=period, guid=source.guid) + "/manifest.json"
            cred = DefaultAzureCredential()
            blob_client = BlobClient.from_blob_url(manifest_url, credential=cred)
            manifest_data = json.loads(blob_client.download_blob().readall())
            run_id = manifest_data.get("runInfo", {}).get("runId")
            report_date_raw = manifest_data.get("runInfo", {}).get("endDate")
            report_date = None
            if report_date_raw:
                report_date = datetime.date.fromisoformat(report_date_raw.split("T")[0])
            if ImportSnapshot.objects.filter(run_id=run_id).exists() and not overwrite:
                source.status = "skipped"
                source.save(update_fields=["last_attempted_at", "status"])
                logger.info("Skip existing run %s for %s", run_id, source.name)
                return
            blob_name = manifest_data.get("blobs", [{}])[0].get("blobName")
            if not blob_name:
                raise RuntimeError("No blobName in manifest")
            csv_url = source.path_template.format(billing_period=period, guid=source.guid) + f"/{Path(blob_name).name}"
            blob_client = BlobClient.from_blob_url(csv_url, credential=cred)
            tmp_dir = Path(self.mktemp())
            gz_path = tmp_dir / Path(blob_name).name
            manifest_path = tmp_dir / "manifest.json"
            with open(gz_path, "wb") as fh:
                fh.write(blob_client.download_blob().readall())
            with open(manifest_path, "w", encoding="utf-8") as fh:
                json.dump(manifest_data, fh)
            import gzip
            csv_path = tmp_dir / (gz_path.stem)
            with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
                f_out.write(f_in.read())
            if dry_run:
                source.status = "dry-run"
                logger.info("Dry run completed for %s. Files stored at %s", source.name, tmp_dir)
            else:
                importer = CostCsvImporter(str(csv_path), run_id=run_id, report_date=report_date, source=source)
                importer.import_file()
                source.last_imported_at = datetime.datetime.utcnow()
                source.status = "imported"
            source.save(update_fields=["last_attempted_at", "last_imported_at", "status"])
        except Exception as exc:
            source.status = f"error: {exc}"
            source.save(update_fields=["last_attempted_at", "status"])
            logger.error("Failed to import from %s: %s", source.name, exc)

