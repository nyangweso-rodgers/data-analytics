# Salesforce

- Salesforce Data Model:

  - **Salesforce** uses a **relational database structure**, not a document store like **MongoDB**.
  - In **Salesforce**, data is stored in **objects**, which are similar to **tables** in a **relational database**. Each **object** has **fields** (**columns**) and **records** (**rows**). For example, a **Customer** record would be a row in the **Contact** or **Account** object.

- **Core Data Structure**

  1. **Object**

     - Equivalent to database tables (e.g., **Account**, **Contact**, **Opportunity**)
     - **Standard Objects**: Predefined by Salesforce (e.g., `Account`, `Contact`).
     - **Custom Objects**: Created by users (e.g., `Product__c`, `Invoice__c`)

  2. **Fields**

     - Columns in a table (e.g., **Name**, **Email**, **LastModifiedDate**).
     - **Standard Fields**: Predefined (e.g., `Contact.Id`, `Account.Name`).
     - **Custom Fields**: User-defined (e.g., `Contact.Loyalty_Points__c`)

  3. **Records**
     - Rows in a table (e.g., a single customer’s data in the `Contact` object).

# Data Management Tools

## 1. APIs

- REST/SOAP APIs to interact with records.

## 2. SOQL (Salesforce Object Query Language)

- SQL-like query language
- Example:
  ```sql
     SELECT Name FROM Contact WHERE LastModifiedDate > 2024-01-01T00:00:00Z
  ```
- You can execute **SOQL** directly within **Salesforce** using several built-in tools and interfaces:

  1.  **Using the Developer Console** (Simplest Method)

      - Go to **Setup** → Type "Developer Console" in Quick Find → Open it.
      - Click the **Query Editor** tab.
      - Write your SOQL query and click **Execute**.
      - Example:
        ```sql
            SELECT Id, Name, Email, LastModifiedDate
            FROM Contact
            WHERE LastModifiedDate = THIS_WEEK
            LIMIT 100
        ```

  2.  **Using Workbench** (**External Tool**)

      - Workbench is a powerful third-party tool for Salesforce admins and developers.
      - Steps:
        - Go to [Workbench](https://workbench.developerforce.com/login.php)
        - Log in with your Salesforce credentials.
        - Navigate to **Utilities** → **REST Explorer**.
        - Write your SOQL query and execute it as a GET request.
      - Example:
        ```sql
            /services/data/v58.0/query?q=SELECT+Id,Name+FROM+Account+LIMIT+10
        ```

  3.  **Using Salesforce CLI** (**Command Line**)

      - Steps:
        1. Install the [Salesforce CLI](https://developer.salesforce.com/tools/salesforcecli)
        2. Authenticate with your Salesforce org:
           ```sh
            #bash
            sfdx auth:web:login -a myorg
           ```
        3. Run a SOQL query:
           ```sh
            #bash
            sfdx force:data:soql:query -q "SELECT Id, Name FROM Account LIMIT 5" -u myorg
           ```

  4.  **Using Apex Code**

      - Embed SOQL in Apex (Salesforce’s programming language) for automation.
      - Example:
        ```java
            List<Contact> contacts = [SELECT Id, Name, Email FROM Contact WHERE AccountId = '001xx000003DGb0'];
            System.debug(contacts);
        ```

  5.  **Using APIs**:

      - For integrations, use Salesforce’s REST or SOAP APIs.
      - **REST API Example**:

        ```py
            import requests

            access_token = "YOUR_ACCESS_TOKEN"
            instance_url = "https://your-instance.salesforce.com"

            query = "SELECT Id, Name FROM Account LIMIT 5"
            response = requests.get(
                f"{instance_url}/services/data/v58.0/query?q={query}",
                headers={"Authorization": f"Bearer {access_token}"}
            )

            print(response.json())
        ```

## 3. Bulk API

- For large data operations.

## 4. Change Data Capture (CDC)

- Streams record changes in real-time (like Kafka topics).

# SOQL Queries

1. Fiter Results By Range of Dates

   - **Example**: (**Greater than or less than a certain time**)

     ```sql
      SELECT Id
      FROM Case
      WHERE CreatedDate >= 2013-12-21T00:00:00Z
      AND CreatedDate <= 2013-12-23T23:59:59Z
     ```

   - **Example**: (**Greater than a certain time**)
     ```sql
      SELECT count(Id)
      FROM Lead
      WHERE createdDate > 2015-02-01T00:00:00Z
     ```
   - **Example** (**Greater than yesterday**)
     ```sql
      SELECT count(Id)
      FROM Lead
      WHERE createdDate > YESTERDAY
     ```
   - **Example** (**Greater than 62 days ago**):
     ```sql
      SELECT id
      FROM Lead
      WHERE CreatedDate = LAST_N_DAYS:62
      LIMIT 100
     ```
   - **Example** (**Greater than last week**):

     ```sql
      SELECT Id
      FROM Lead
      WHERE createdDate > LAST_WEEK
     ```

   - **Example** (**This year**):

     ```sql
      SELECT Id
      FROM Lead
      WHERE CALENDAR_YEAR(CreatedDate) = 2025
     ```

   - **Example** (**Get the current Year returned**):
     ```sql
      SELECT CALENDAR_YEAR(CreatedDate)
      FROM Opportunity
      group by CALENDAR_YEAR(CreatedDate)
     ```

# Resources and Further Reading
