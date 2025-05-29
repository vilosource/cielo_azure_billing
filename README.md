# cielo_azure_billing
Service to provide azure billing analysis information.

When requesting cost entries for a specific billing date via `/api/v1/cost-entries/?date=YYYY-MM-DD`,
the API automatically returns data from the most recent import snapshot that includes that date.

## API Filtering

Cost entry endpoints accept the following query parameters for filtering:

- `resourceGroupName`
- `subscriptionName`
- `meterCategory`
- `meterSubCategory`
- `serviceFamily`
- `resourceLocation`
- `chargeType`
- `pricingModel`
- `publisherName`
- `costCenter`
- `tags`
