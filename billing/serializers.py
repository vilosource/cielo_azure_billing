from rest_framework import serializers
from .models import ImportSnapshot, Customer, Subscription, Resource, Meter, CostEntry


class ImportSnapshotSerializer(serializers.ModelSerializer):
    class Meta:
        model = ImportSnapshot
        fields = '__all__'


class CustomerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Customer
        fields = '__all__'


class SubscriptionSerializer(serializers.ModelSerializer):
    customer = CustomerSerializer(read_only=True)

    class Meta:
        model = Subscription
        fields = '__all__'


class ResourceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Resource
        fields = '__all__'


class MeterSerializer(serializers.ModelSerializer):
    class Meta:
        model = Meter
        fields = '__all__'


class CostEntrySerializer(serializers.ModelSerializer):
    subscription = SubscriptionSerializer(read_only=True)
    resource = ResourceSerializer(read_only=True)
    meter = MeterSerializer(read_only=True)
    snapshot = ImportSnapshotSerializer(read_only=True)

    class Meta:
        model = CostEntry
        fields = '__all__'
