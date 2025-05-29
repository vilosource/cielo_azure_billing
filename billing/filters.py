from django_filters import rest_framework as filters
from .models import CostEntry


class CostEntryFilter(filters.FilterSet):
    resourceGroupName = filters.CharFilter(field_name='resource__resource_group', lookup_expr='iexact')
    subscriptionName = filters.CharFilter(field_name='subscription__name', lookup_expr='iexact')
    meterCategory = filters.CharFilter(field_name='meter__category', lookup_expr='iexact')
    meterSubCategory = filters.CharFilter(field_name='meter__subcategory', lookup_expr='iexact')
    serviceFamily = filters.CharFilter(field_name='meter__service_family', lookup_expr='iexact')
    resourceLocation = filters.CharFilter(field_name='resource__location', lookup_expr='iexact')
    chargeType = filters.CharFilter(field_name='charge_type', lookup_expr='iexact')
    pricingModel = filters.CharFilter(field_name='pricing_model', lookup_expr='iexact')
    publisherName = filters.CharFilter(field_name='publisher_name', lookup_expr='iexact')
    costCenter = filters.CharFilter(field_name='cost_center', lookup_expr='iexact')
    tags = filters.CharFilter(method='filter_tags')

    class Meta:
        model = CostEntry
        fields = []

    def filter_tags(self, queryset, name, value):
        if value:
            return queryset.filter(tags__contains=value)
        return queryset
