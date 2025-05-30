import csv
import logging
import datetime
from .models import CostReportSnapshot, Customer, Subscription, Resource, Meter, CostEntry

logger = logging.getLogger(__name__)

def parse_date(date_str):
    """Convert date from MM/DD/YYYY to a date object."""
    try:
        if date_str:
            # Try to parse the date in MM/DD/YYYY format
            dt = datetime.datetime.strptime(date_str, '%m/%d/%Y')
            return dt.date()  # Return date object instead of string
        return None
    except ValueError:
        logger.error('Invalid date format: %s', date_str)
        return None

class CostCsvImporter:
    def __init__(self, file_path, run_id=None, report_date=None, source=None):
        self.file_path = file_path
        self.run_id = run_id
        self.report_date = report_date
        self.source = source

    def import_file(self):
        logger.info('Starting import from %s', self.file_path)
        try:
            snapshot = CostReportSnapshot.objects.create(
                file_name=self.file_path,
                run_id=self.run_id or str(datetime.datetime.utcnow().timestamp()),
                report_date=self.report_date,
                source=self.source,
                status=CostReportSnapshot.Status.IN_PROGRESS,
            )
            logger.info('Created snapshot record: %s', snapshot.id)
        except Exception as e:
            logger.error('Failed to create snapshot: %s', e)
            raise

        try:
            with open(self.file_path, newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                count = 0
                for row in reader:
                    try:
                        customer, created = Customer.objects.get_or_create(
                            tenant_id=row.get('customerTenantId')
                        )
                        if created:
                            logger.info('Created new customer: %s', customer.tenant_id)

                        subscription, created = Subscription.objects.get_or_create(
                            subscription_id=row.get('SubscriptionId'),
                            defaults={'name': row.get('subscriptionName'), 'customer': customer},
                        )
                        if created:
                            logger.info('Created new subscription: %s', subscription.subscription_id)
                        elif row.get('subscriptionName') and subscription.name != row.get('subscriptionName'):
                            subscription.name = row.get('subscriptionName')
                            subscription.save(update_fields=['name'])

                        resource_id_val = row.get('ResourceId')
                        resource_name_val = None
                        if resource_id_val:
                            resource_name_val = resource_id_val.rstrip('/')\
                                .split('/')[-1]

                        rg_raw = row.get('resourceGroupName')
                        rg_val = rg_raw.strip().lower() if rg_raw else None

                        resource, created = Resource.objects.get_or_create(
                            resource_id=resource_id_val,
                            defaults={
                                'name': row.get('productOrderName'),
                                'resource_name': resource_name_val,
                                'resource_group': rg_val,
                                'location': row.get('resourceLocation'),
                            },
                        )
                        update_fields = []
                        if resource_name_val and not resource.resource_name:
                            resource.resource_name = resource_name_val
                            update_fields.append('resource_name')
                        if rg_val and resource.resource_group != rg_val:
                            resource.resource_group = rg_val
                            update_fields.append('resource_group')
                        if update_fields:
                            resource.save(update_fields=update_fields)
                        if created:
                            logger.info('Created new resource: %s', resource.resource_id)

                        meter, created = Meter.objects.get_or_create(
                            meter_id=row.get('meterId'),
                            defaults={
                                'name': row.get('meterName'),
                                'category': row.get('meterCategory'),
                                'subcategory': row.get('meterSubCategory'),
                                'service_family': row.get('serviceFamily'),
                                'unit': row.get('unitOfMeasure'),
                            },
                        )
                        if created:
                            logger.info('Created new meter: %s', meter.meter_id)
                        elif row.get('serviceFamily') and meter.service_family != row.get('serviceFamily'):
                            meter.service_family = row.get('serviceFamily')
                            meter.save(update_fields=['service_family'])

                        # Parse the date using our helper function
                        parsed_date = parse_date(row.get('date'))
                        if not parsed_date:
                            logger.error('Failed to parse date for row with date: %s', row.get('date'))
                            continue

                        cost_entry = CostEntry.objects.create(
                            snapshot=snapshot,
                            date=parsed_date,
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
                            publisher_name=row.get('publisherName'),
                            cost_center=row.get('costCenter'),
                            tags=row.get('tags') or None,
                        )
                        logger.debug('Created cost entry: %s', cost_entry.id)
                        count += 1
                    except Exception as e:
                        logger.error('Error processing row: %s. Row data: %s', e, {k: v for k, v in row.items() if k in ['date', 'costInUsd', 'SubscriptionId', 'ResourceId']})
                        continue

            snapshot.status = CostReportSnapshot.Status.COMPLETE
            snapshot.save(update_fields=['status'])
            logger.info('Imported %s entries from %s', count, self.file_path)
            return count
        except Exception as e:
            snapshot.status = CostReportSnapshot.Status.FAILED
            snapshot.save(update_fields=['status'])
            logger.error('Import failed: %s', e)
            raise
