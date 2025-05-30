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

    # New parameter names matching summary endpoints
    subscription_id = filters.CharFilter(field_name='subscription__subscription_id', lookup_expr='iexact')
    resource_group = filters.CharFilter(field_name='resource__resource_group', lookup_expr='iexact')
    location = filters.CharFilter(field_name='resource__location', lookup_expr='iexact')
    meter_category = filters.CharFilter(field_name='meter__category', lookup_expr='iexact')
    meter_subcategory = filters.CharFilter(field_name='meter__subcategory', lookup_expr='iexact')
    pricing_model = filters.CharFilter(field_name='pricing_model', lookup_expr='iexact')
    publisher_name = filters.CharFilter(field_name='publisher_name', lookup_expr='iexact')
    resource_name = filters.CharFilter(field_name='resource__resource_name', lookup_expr='iexact')
    min_cost = filters.NumberFilter(field_name='cost_in_usd', lookup_expr='gte')
    max_cost = filters.NumberFilter(field_name='cost_in_usd', lookup_expr='lte')
    source_id = filters.NumberFilter(field_name='snapshot__source_id')
    tag_key = filters.CharFilter(method='filter_tag_key')
    tag_value = filters.CharFilter(method='filter_tag_value')

    class Meta:
        model = CostEntry
        fields = []

    def filter_tags(self, queryset, name, value):
        if value:
            return queryset.filter(tags__contains=value)
        return queryset

    def filter_tag_key(self, queryset, name, value):
        if value:
            return queryset.filter(tags__has_key=value)
        return queryset

    def filter_tag_value(self, queryset, name, value):
        key = self.data.get('tag_key')
        if key and value:
            return queryset.filter(tags__contains={key: value})
        return queryset


class CostSummaryFilter(filters.FilterSet):
    subscription_id = filters.CharFilter(field_name='subscription__subscription_id', lookup_expr='iexact')
    resource_group = filters.CharFilter(field_name='resource__resource_group', lookup_expr='iexact')
    location = filters.CharFilter(field_name='resource__location', lookup_expr='iexact')
    meter_category = filters.CharFilter(field_name='meter__category', lookup_expr='iexact')
    meter_subcategory = filters.CharFilter(field_name='meter__subcategory', lookup_expr='iexact')
    pricing_model = filters.CharFilter(field_name='pricing_model', lookup_expr='iexact')
    publisher_name = filters.CharFilter(field_name='publisher_name', lookup_expr='iexact')
    resource_name = filters.CharFilter(field_name='resource__resource_name', lookup_expr='iexact')
    min_cost = filters.NumberFilter(field_name='cost_in_usd', lookup_expr='gte')
    max_cost = filters.NumberFilter(field_name='cost_in_usd', lookup_expr='lte')
    source_id = filters.NumberFilter(field_name='snapshot__source_id')
    tag_key = filters.CharFilter(method='filter_tag_key')
    tag_value = filters.CharFilter(method='filter_tag_value')

    class Meta:
        model = CostEntry
        fields = []

    def filter_tag_key(self, queryset, name, value):
        if value:
            return queryset.filter(tags__has_key=value)
        return queryset

    def filter_tag_value(self, queryset, name, value):
        key = self.data.get('tag_key')
        if key and value:
            return queryset.filter(tags__contains={key: value})
        return queryset
