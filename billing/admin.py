from django.contrib import admin
from .models import ImportSnapshot, Customer, Subscription, Resource, Meter, CostEntry


@admin.register(ImportSnapshot)
class ImportSnapshotAdmin(admin.ModelAdmin):
    list_display = ('file_name', 'snapshot_date', 'created_at')
    search_fields = ('file_name',)


@admin.register(Customer)
class CustomerAdmin(admin.ModelAdmin):
    search_fields = ('tenant_id', 'name')


@admin.register(Subscription)
class SubscriptionAdmin(admin.ModelAdmin):
    list_display = ('subscription_id', 'name', 'customer')
    list_filter = ('customer',)
    search_fields = ('subscription_id', 'name')


@admin.register(Resource)
class ResourceAdmin(admin.ModelAdmin):
    list_display = ('resource_id', 'name', 'resource_group', 'location')
    search_fields = ('resource_id', 'name', 'resource_group', 'location')


@admin.register(Meter)
class MeterAdmin(admin.ModelAdmin):
    list_display = ('meter_id', 'name', 'category', 'subcategory', 'service_family', 'unit')
    search_fields = ('meter_id', 'name', 'category', 'service_family')


@admin.register(CostEntry)
class CostEntryAdmin(admin.ModelAdmin):
    list_display = (
        'date',
        'subscription',
        'resource',
        'meter',
        'charge_type',
        'pricing_model',
        'publisher_name',
        'cost_center',
        'cost_in_usd',
    )
    list_filter = (
        'snapshot',
        'date',
        'subscription',
        'resource__resource_group',
        'resource__location',
        'meter__category',
        'meter__subcategory',
        'meter__service_family',
        'charge_type',
        'pricing_model',
        'publisher_name',
        'cost_center',
        'tags',
    )
    search_fields = (
        'subscription__name',
        'resource__name',
        'resource__resource_group',
        'resource__location',
        'meter__category',
        'meter__subcategory',
        'meter__service_family',
        'publisher_name',
        'cost_center',
        'tags',
    )
    readonly_fields = ('snapshot',)
