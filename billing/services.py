import csv
import logging
from .models import ImportSnapshot, Customer, Subscription, Resource, Meter, CostEntry

logger = logging.getLogger(__name__)


class CostCsvImporter:
    def __init__(self, file_path):
        self.file_path = file_path

    def import_file(self):
        snapshot = ImportSnapshot.objects.create(file_name=self.file_path)
        with open(self.file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            count = 0
            for row in reader:
                customer, _ = Customer.objects.get_or_create(
                    tenant_id=row.get('customerTenantId')
                )

                subscription, _ = Subscription.objects.get_or_create(
                    subscription_id=row.get('SubscriptionId'),
                    defaults={'name': row.get('subscriptionName'), 'customer': customer},
                )

                resource, _ = Resource.objects.get_or_create(
                    resource_id=row.get('ResourceId'),
                    defaults={
                        'name': row.get('productOrderName'),
                        'resource_group': row.get('resourceGroupName'),
                        'location': row.get('resourceLocation'),
                    },
                )

                meter, _ = Meter.objects.get_or_create(
                    meter_id=row.get('meterId'),
                    defaults={
                        'name': row.get('meterName'),
                        'category': row.get('meterCategory'),
                        'subcategory': row.get('meterSubCategory'),
                        'unit': row.get('unitOfMeasure'),
                    },
                )

                CostEntry.objects.create(
                    snapshot=snapshot,
                    date=row.get('date'),
                    subscription=subscription,
                    resource=resource,
                    meter=meter,
                    cost_in_usd=row.get('costInUsd') or 0,
                    cost_in_billing_currency=row.get('costInBillingCurrency') or 0,
                    billing_currency=row.get('billingCurrency'),
                    quantity=row.get('quantity') or 0,
                    unit_price=row.get('unitPrice') or 0,
                    payg_price=row.get('PayGPrice') or 0,
                    pricing_model=row.get('pricingModel'),
                    charge_type=row.get('chargeType'),
                    tags=row.get('tags') or None,
                )
                count += 1

        logger.info('Imported %s entries from %s', count, self.file_path)
        return count
