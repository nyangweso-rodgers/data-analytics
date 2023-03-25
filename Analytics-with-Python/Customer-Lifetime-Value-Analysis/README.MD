# Customer Lifetime Value (CLV) Analysis

## Table Of Contents
- [Further Reading]()
  - [What’s a Customer Worth? Modelling Customers Lifetime Value For Non-Contractual Business with Python](https://towardsdatascience.com/whats-a-customer-worth-8daf183f8a4f)
  - [Predicting customer lifetime value using the Pareto/NBD model and Gamma-Gamma model](https://www.mikulskibartosz.name/predicting-customer-lifetime-value-using-the-pareto-nbd-model-and-gamma-gamma-model/)
  - [https://lifetimes.readthedocs.io/en/latest/Quickstart.html](https://lifetimes.readthedocs.io/en/latest/Quickstart.html)
  - [Identify Your High-Value Customer](https://medium.com/swlh/identify-your-high-value-customer-7b8868b65554)


# CLV Model Definition
* For the CLV models, the following nomenclature is used:
  * Frequency represents the number of repeat purchases the customer has made. This means that it’s one less than the total number of purchases.
  * T represents the age of the customer in whatever time units chosen (daily, in our dataset). This is equal to the duration between a customer’s first purchase and the end of the period under study.
  * Recency represents the age of the customer when they made their most recent purchases. This is equal to the duration between a customer’s first purchase and their latest purchase. (Thus if they have made only 1 purchase, the recency is 0.)