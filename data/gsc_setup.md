
# Google Search Console Setup

This guide helps you set up Google Search Console integration for enhanced SEO attribution using real click data instead of just ranking positions.

## Benefits of GSC Integration

- **Real Click Data**: Attribution based on actual search clicks, not just rankings
- **Higher Confidence**: More accurate attribution with verified user behavior
- **Time-based Matching**: Match search queries to lead inquiry timestamps
- **Performance Metrics**: Access to impressions, CTR, and position data

## Setup Steps

### 1. Enable Search Console API

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Navigate to **APIs & Services** > **Library**
4. Search for "Google Search Console API"
5. Click **Enable**

### 2. Create Service Account

1. In Google Cloud Console, go to **IAM & Admin** > **Service Accounts**
2. Click **Create Service Account**
3. Enter service account details:
   - Name: `gsc-attribution-service`
   - Description: `Service account for traffic attribution analysis`
4. Click **Create and Continue**
5. Skip role assignment (no Google Cloud roles needed)
6. Click **Done**

### 3. Generate Credentials

1. Click on the created service account
2. Go to **Keys** tab
3. Click **Add Key** > **Create new key**
4. Select **JSON** format
5. Click **Create** to download the credentials file

### 4. Add Service Account to GSC Property

1. Go to [Google Search Console](https://search.google.com/search-console)
2. Select your property (website)
3. Click **Settings** (gear icon) in the left sidebar
4. Click **Users and permissions**
5. Click **Add user**
6. Enter the service account email (from the JSON file)
7. Set permission to **Full** (read access needed)
8. Click **Add**

### 5. Configure in Replit

Choose one of these options:

#### Option A: Replit Secrets (Recommended)
1. In Replit, open the **Secrets** tab
2. Add these secrets:
   - `GSC_CREDENTIALS_PATH`: `data/gsc_credentials.json`
   - `GSC_PROPERTY_URL`: Your website URL (e.g., `https://example.com/`)

#### Option B: Local File
1. Upload the credentials JSON file to `/data/gsc_credentials.json`
2. The system will auto-detect and use it

## Testing Integration

Run the test script to verify setup:

```bash
python test_attribution.py
```

Look for GSC integration test results.

## Troubleshooting

### Common Issues

**"Property not accessible"**
- Verify service account email is added to GSC property
- Check property URL format (must include https:// and trailing /)

**"Access denied"**
- Ensure Search Console API is enabled
- Verify service account has proper permissions

**"No data available"**
- GSC data has 2-3 day delay
- Ensure property has search traffic
- Check date range (GSC keeps 16 months of data)

### Support

Check the attribution logs for detailed error messages. The system will automatically fall back to CSV ranking data if GSC is unavailable.
