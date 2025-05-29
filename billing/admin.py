from django.contrib import admin
from .models import ImportSnapshot, Customer, Subscription, Resource, Meter, CostEntry


@admin.register(ImportSnapshot)
class ImportSnapshotAdmin(admin.ModelAdmin):
    list_display = ('file_name', 'snapshot_date')
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
    list_display = ('meter_id', 'name', 'category', 'subcategory', 'unit')
    search_fields = ('meter_id', 'name', 'category')


@admin.register(CostEntry)
class CostEntryAdmin(admin.ModelAdmin):
    list_display = ('date', 'subscription', 'resource', 'meter', 'cost_in_usd')
    list_filter = ('snapshot', 'date', 'subscription', 'resource')
    search_fields = ('subscription__name', 'resource__name')
    readonly_fields = ('snapshot',)
