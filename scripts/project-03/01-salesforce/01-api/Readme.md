# Salesforce API

# Types of Salesforce APIs

## 1. REST API

- **Features**:

  1. Performance: Processes records one at a time or in small batches.
  2. Data Format: JSON (REST) or XML (SOAP).
  3. Batch Size: Typically processes one record per request.
  4. Processing Mode: Synchronous (requests are processed immediately).
  5. Daily API Limits: Lower limits for individual requests (e.g., 239,000 API calls/day).
  6. API Concurrency Limits: Limited by API request limits.

- Use Case:
  1. Handling smaller, real-time interactions (e.g., CRUD operations).
  2. Real-Time Integration: Ideal for real-time integrations (e.g., syncing data with external systems).

## 2. Bulk API

- **Features**:

  1. Performance: Processes large datasets in batches (up to 10,000 records per batch).
  2. Data Format: CSV, XML, or JSON.
  3. Batch Size: Up to 10,000 records per batch.
  4. Processing Mode: Asynchronous (jobs are submitted and processed in the background).
  5. Daily API Limits: Higher limits for bulk operations (e.g., 15,000 batches per day).
  6. API Concurrency Limits: Supports multiple concurrent jobs.

- Use Case:
  1. Handling large volumes of data (e.g., millions of records).
  2. Data Migration: Ideal for migrating large datasets (e.g., millions of records).

# Resources and Further Reading
