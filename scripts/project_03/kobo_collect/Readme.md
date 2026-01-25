# Kobo Tool Box

## Table Of Contents

# How to Fetch Data From KoboToolbox

## 1. REST Service (Webhook/Push) - Optional

- REST Services allow KoboToolbox to automatically push new submissions to your endpoint when they're created, but this only sends data submitted via mobile, not data edited in the Kobo web app OpenfnKoboToolbox. This is for push-based integration.
- **When to Use REST Services**: You'd only set up a REST Service if you want:
  1. Real-time push notifications when new submissions arrive
  2. To avoid polling the API repeatedly
  3. To receive data immediately after mobile submission

## 2. API Direct Fetch (Pull)

- You can directly fetch data from **KoboToolbox** using the API endpoint without setting up REST Services - you just need your **API token** and the form's **asset UID OpenfnKoboToolbox**. This is for pull-based integration.
- To use this approach, you need:
  1.  **API Token** - Get from Account Settings → Security
  2.  **Form API URL** - Usually in format: `https://[server]/api/v2/assets/[asset-uid]/data/`
      - You can find the project asset UID in the URL of your project summary page. It is the string of letters and numbers that appears after "forms/" in the URL
      - Log into KoboToolbox
      - Open your form/project
      - Click on the "Summary" tab
      - Look at the URL in your browser - it will look like:
        ```sh
          https://kf.kobotoolbox.org/#/forms/aBcDeFgHiJkLmNoPqRsTuVwXyZ/summary
        ```
      - The asset UID is: `aBcDeFgHiJkLmNoPqRsTuVwXyZ`
  3.  Proper authentication headers

# Steps

1. API Access:
   - You’ll need to authenticate with the **KoboToolbox API** using an **API token**, which you can generate from your **KoboToolbox** account settings.
   - Access API Token:
     1. Method 1: `https://kf.kobotoolbox.org/token/?format=json`
     2. Method 2: `curl -u <username>:<password> "https:/kf.kobotoolbox.org/token/?format=json"`

# API Endpoints

## 1. Asset List Endpoint

- Call the assets list endpoint to see all your forms:
  ```sh
    curl -H "Authorization: Token YOUR_API_TOKEN" \
      https://kf.kobotoolbox.org/api/v2/assets/
  ```
- This returns a JSON with all your forms. Look for your form name and find the uid field.

# Resources and Further Reading
