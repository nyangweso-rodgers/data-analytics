# RFM Analysis in BigQuery

## Table Of Contents

# Theory of RFM
* __Recency-Frequency-Monetary__ (RFM) analysis is a indexing technique that uses past purchase behavior to _segment customers_. Given a table with purchase transaction data, we calculate a score based on:   
    1.  _how recently the customer purchased_, 
    2. _how often they make purchases_ and 
    3. _how much they spend on average on each purchase_. 

* Using the above scores, we can segment our customer list to:
    1. _identify our most loyal customers by segmenting one-time buyers from customers with repeat purchases_
    2. _increase customer retention and lifetime value_
    3. _increase average order size, etc._

# Example
* Suppose we have a _sales_ table within _transactions_ dataset in Bigquery with the following columns:
    1. shop_id
    2. sales_date
    3. order_id
    4. delivery_id
    5. item_name
    6. item_uom
    7. amount,

we can write an SQL script to automate the __RFM Score__ as follows: