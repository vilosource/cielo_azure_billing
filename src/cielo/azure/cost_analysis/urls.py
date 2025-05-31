from rest_framework.routers import DefaultRouter
from django.urls import path, include
from .views import (
    CostReportSnapshotViewSet,
    CustomerViewSet,
    SubscriptionViewSet,
    ResourceViewSet,
    MeterViewSet,
    CostEntryViewSet,
    SubscriptionSummaryView,
    VirtualMachineSummaryView,
    ResourceGroupSummaryView,
    MeterCategorySummaryView,
    RegionSummaryView,
    AvailableReportDatesView,
    SnapshotReportDatesView,
    ResourceGroupTotalsView,
)

router = DefaultRouter()
router.register('snapshots', CostReportSnapshotViewSet)
router.register('customers', CustomerViewSet)
router.register('subscriptions', SubscriptionViewSet)
router.register('resources', ResourceViewSet)
router.register('meters', MeterViewSet)
router.register('cost-entries', CostEntryViewSet)

urlpatterns = [
    path('', include(router.urls)),
    path('costs/subscription-summary/', SubscriptionSummaryView.as_view()),
    path('costs/virtual-machines-summary/', VirtualMachineSummaryView.as_view()),
    path('costs/resource-group-summary/', ResourceGroupSummaryView.as_view()),
    path('costs/meter-category-summary/', MeterCategorySummaryView.as_view()),
    path('costs/region-summary/', RegionSummaryView.as_view()),
    path('costs/available-report-dates/', AvailableReportDatesView.as_view()),
    path('costs/resource-group-totals/', ResourceGroupTotalsView.as_view()),
    path('reports/available-report-dates/', SnapshotReportDatesView.as_view()),
]
