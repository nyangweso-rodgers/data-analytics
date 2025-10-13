# Netsuite

## Table Of Contents

# NetSuite API Overview

- NetSuite API Types:

  1.  REST API
      - RESTful Web Services (REST WS) - newer, more modern
      - SuiteTalk REST API - current preferred method
  2.  SOAP API ((Legacy but widely used))
      - SuiteTalk SOAP Web Services
      - Still very common in existing integrations

- Authentication
  - You need:
    1. `consumer_key`
    2. `consumer_secret`
    3. `token_id`
    4. `token_secret`
    5. `account_id`

# NetSuite Records

- Common NetSuite Record Types:
  1. Customer
  2. Sales Order
  3. Invoice
  4. Item
  5. Contact
  6. Vendor
  7. Custom Records

# Clinets

## 1. Python

- Key Python Libraries

  ```py
    # Popular libraries for NetSuite integration
    pip install netsuitesdk  # Third-party SDK
    pip install python-netsuite  # Another option
    # Or use direct REST calls with requests library
  ```

- Python Integration Example

  ```py
    import requests
    from requests_oauthlib import OAuth1
    import json

    # Authentication
    auth = OAuth1(
        client_key='your_consumer_key',
        client_secret='your_consumer_secret',
        resource_owner_key='your_token_id',
        resource_owner_secret='your_token_secret',
        signature_method='HMAC-SHA256'
    )

    # Base URL
    base_url = f"https://{account_id}.suitetalk.api.netsuite.com/services/rest/record/v1"

    # Fetch a customer record
    def get_customer(customer_id):
        url = f"{base_url}/customer/{customer_id}"
        response = requests.get(url, auth=auth)
        return response.json()

    # Create a sales order
    def create_sales_order(order_data):
        url = f"{base_url}/salesOrder"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, json=order_data, auth=auth, headers=headers)
        return response.json()
  ```

# Resources and Further Reading
