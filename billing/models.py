from django.db import models


class ImportSnapshot(models.Model):
    snapshot_date = models.DateField(auto_now_add=True, db_index=True)
    file_name = models.CharField(max_length=255)

    def __str__(self):
        return f'{self.file_name} @ {self.snapshot_date}'


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
    cost_in_billing_currency = models.DecimalField(max_digits=12, decimal_places=4, null=True, blank=True)
    billing_currency = models.CharField(max_length=10, null=True, blank=True)
    quantity = models.DecimalField(max_digits=12, decimal_places=4)
    unit_price = models.DecimalField(max_digits=12, decimal_places=6)
    payg_price = models.DecimalField(max_digits=12, decimal_places=6, null=True, blank=True)
    pricing_model = models.CharField(max_length=64, null=True, blank=True)
    charge_type = models.CharField(max_length=64, null=True, blank=True)
    tags = models.JSONField(null=True, blank=True)

    def __str__(self):
        return f'{self.date} - {self.subscription}'
