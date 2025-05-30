import datetime
import json
import logging
from pathlib import Path
from urllib.parse import urlparse
from django.core.management.base import BaseCommand
from billing.models import BillingBlobSource, ImportSnapshot
from billing.services import CostCsvImporter

try:
    from azure.identity import DefaultAzureCredential
    from azure.storage.blob import BlobClient, ContainerClient
except Exception:  # pragma: no cover - libs optional for offline env
    DefaultAzureCredential = None
    BlobClient = None
    ContainerClient = None

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

    def _parse_base_folder(self, base):
        parsed = urlparse(base)
        path = parsed.path.lstrip("/")
        parts = path.split("/", 1)
        container = parts[0]
        prefix = ""
        if len(parts) > 1:
            prefix = parts[1].rstrip("/") + "/"
        container_url = f"{parsed.scheme}://{parsed.netloc}/{container}"
        return container_url, prefix

    def _process_source(self, source, period, dry_run, overwrite):
        source.last_attempted_at = datetime.datetime.utcnow()
        try:
            if DefaultAzureCredential is None:
                raise RuntimeError("Azure SDK not installed")

            cred = DefaultAzureCredential()
            container_url, prefix = self._parse_base_folder(source.base_folder)
            client = ContainerClient.from_container_url(container_url, credential=cred)

            listing_prefix = prefix
            if period:
                listing_prefix = f"{prefix}{period}/"

            blobs = client.list_blobs(name_starts_with=listing_prefix)
            manifests = [b for b in blobs if b.name.endswith("manifest.json")]
            if not manifests:
                source.status = "no-manifests"
                source.save(update_fields=["last_attempted_at", "status"])
                logger.info("No manifests found for %s", source.name)
                return

            for blob in manifests:
                manifest_url = f"{container_url}/{blob.name}"
                bclient = BlobClient.from_blob_url(manifest_url, credential=cred)
                manifest_data = json.loads(bclient.download_blob().readall())
                run_id = manifest_data.get("runInfo", {}).get("runId")
                report_date_raw = manifest_data.get("runInfo", {}).get("endDate")
                report_date = None
                if report_date_raw:
                    report_date = datetime.date.fromisoformat(
                        report_date_raw.split("T")[0]
                    )
                if (
                    ImportSnapshot.objects.filter(run_id=run_id).exists()
                    and not overwrite
                ):
                    logger.info("Skip existing run %s for %s", run_id, source.name)
                    continue

                blob_name = manifest_data.get("blobs", [{}])[0].get("blobName")
                if not blob_name:
                    raise RuntimeError("No blobName in manifest")
                csv_url = f"{container_url}/{blob_name}"
                bclient = BlobClient.from_blob_url(csv_url, credential=cred)

                tmp_dir = Path(self.mktemp())
                gz_path = tmp_dir / Path(blob_name).name
                manifest_path = tmp_dir / "manifest.json"
                with open(gz_path, "wb") as fh:
                    fh.write(bclient.download_blob().readall())
                with open(manifest_path, "w", encoding="utf-8") as fh:
                    json.dump(manifest_data, fh)
                import gzip

                csv_path = tmp_dir / (gz_path.stem)
                with gzip.open(gz_path, "rb") as f_in, open(csv_path, "wb") as f_out:
                    f_out.write(f_in.read())
                if dry_run:
                    source.status = "dry-run"
                    logger.info(
                        "Dry run completed for %s. Files stored at %s",
                        source.name,
                        tmp_dir,
                    )
                else:
                    importer = CostCsvImporter(
                        str(csv_path),
                        run_id=run_id,
                        report_date=report_date,
                        source=source,
                    )
                    importer.import_file()
                    source.last_imported_at = datetime.datetime.utcnow()
                    source.status = "imported"
                source.save(
                    update_fields=["last_attempted_at", "last_imported_at", "status"]
                )
        except Exception as exc:
            source.status = f"error: {exc}"
            source.save(update_fields=["last_attempted_at", "status"])
            logger.error("Failed to import from %s: %s", source.name, exc)
