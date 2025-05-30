from django.db import models


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


class ImportSnapshotQuerySet(models.QuerySet):
    def latest_per_subscription(self):
        from django.db.models import Max

        latest = CostEntry.objects.values("subscription_id").annotate(
            latest_id=Max("snapshot_id")
        )
        return self.filter(id__in=[item["latest_id"] for item in latest])

    def latest_overall(self):
        return self.order_by("-created_at").first()

    def for_day(self, target_date):
        return self.filter(report_date=target_date)


class ImportSnapshot(models.Model):
    run_id = models.CharField(max_length=64, unique=True, db_index=True)
    report_date = models.DateField(null=True, blank=True)
    file_name = models.CharField(max_length=255)
    source = models.ForeignKey(
        "BillingBlobSource", null=True, on_delete=models.SET_NULL
    )
    created_at = models.DateTimeField(auto_now_add=True)

    objects = ImportSnapshotQuerySet.as_manager()

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
    name = models.CharField(max_length=255, null=True, blank=True)
    resource_group = models.CharField(max_length=255, null=True, blank=True)
    location = models.CharField(max_length=255, null=True, blank=True)

    def __str__(self):
        return self.name or self.resource_id


class Meter(models.Model):
    meter_id = models.CharField(max_length=64, unique=True)
    name = models.CharField(max_length=255)
    category = models.CharField(max_length=255)
    subcategory = models.CharField(max_length=255, null=True, blank=True)
    service_family = models.CharField(max_length=255, null=True, blank=True)
    unit = models.CharField(max_length=64)

    def __str__(self):
        return self.name


class CostEntry(models.Model):
    snapshot = models.ForeignKey(ImportSnapshot, on_delete=models.CASCADE)
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
