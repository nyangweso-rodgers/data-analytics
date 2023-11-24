# Analytics in MongoDB

![](images/mongodb-image.png)

## Table Of Contents

- [Further Reading]()
  1.

# Introduction to MongoDB

# The MongoDB Query API

## `db.collection.findOne()`

- Match a single document.

### Find One document

```sql
  db.collection.findOne({
    "status":"DELIVERED"
    });
```

## `db.collection.find()`

- Match, sort, or count multiple documents
- Query 1:
  - find `PAID` transactions and limit to fields you want
    ```sh
      db.collection.find({"status":"PAID"}, {id, status})
    ```
- Query 2:
  - using `sort`
    ```sh
        db.collection.find({"status":"PAID"}).sort({"amount": -1})
    ```

## `db.collection.aggregate()`

- Match, combine and aggregate documents.

# Performing Queries using `.count()`

- Query 1:
  - count transactions with status `COMPLETED`
    ```sh
      #
      db.collection.find({"status":"DELIVERED"}).count()
    ```

# Performing Range Queries using `>=` & `<=` (`$gt` and `$lt`)

- find documents in a collection by a range query.

- Query 1:
  - find transactions with `amount` **greater than** 1000
    ```sh
      # find transactions with amount greater than 1000
      db.collection.find({  amount: {$gt:1000 }})
    ```
- Query 2:
  - find transactions with `amount` **greater than or equal** to 1000
    ```sh
      # find transactions with amount greater than  or equal to 1000
      db.collection.find({ amount: {$gte:1000  }})
    ```
- Query 3:
  - find transactions with `amount` **less than** to 1000
    ```sh
      # find transactions with amount less than 1000
      db.collection.find({ amount: {$lt : 1000  }})
    ```
- Query 4:
  - find transactions with `amount` **less than or equal to** to 1000
    ```sh
      # find transactions with amount less than or equal to 1000
      db.collection.find({ amount: {$lte : 1000  }})
    ```
- Query 5:
  - find transactions with `amount` greater than 1000 and less than 2000
    ```sh
      # find transactions with amount greater than 1000
      db.collection.find({amount: {$gt:1000, $lt:2000}})
    ```
- Task 3:
  - find transactions with `amount` greater than 1000 and less than 2000 from `country` `KENYA`
    ```sh
      # find transactions with amount greater than 1000
      db.collection.find({
        amount: {$gt:1000, $lt:2000},
        country: 'KENYA'
      })
    ```

# Performing Queries using `$in` and `$nin`

- `$in` the queries should return elements where the value of a field is in a specified array of values
- `$nin` return elements where the value of a field is not in the specified range of values.

- Query 2.1:

  - find `users` whose `first_name` are in the specified array.
    ```sh
      db.users.find({ first_name: { $in: ["Rodgers", "Nyangweso"]}})
    ```

- Query 2.2:
  - find `users` whose `first_name` are not in the specified array.
    ```sh
      db.users.find({ first_name: { $nin: ["Rodgers", "Nyangweso"]}})
    ```

# Performing Queries using `$or` & `$and`

- `$or` operator returns documents that match either of the queries passed in the array of queries.
- `$and` operator returns documents that match all of the queries passed in the array of queries.
  - You may decide to use a comma, instead of the `$and` operator when specifying a separated list of expressions. Because **MongoDB** infers an implicit `$and` operation when a comma, is used to separate a list of expressions.
- Query 3.1:

  - find `users` whose `active_status` is `true` or `age` less than 50
    ```sh
      #
      db.users.find({ $or: [{ active_status: 'true}, { age: { $lt: 50}}]})
    ```

- Query 3.2:
  - find `users` whose `active_status` is `true` and `age` less than 50
    ```sh
      #
      db.users.find({ $and: [{ active_status: 'true}, { age: { $lt: 50}}]})
    ```
  - alternatively,
    ```sh
      #
      db.users.find({[{ active_status: 'true}, { age: { $lt: 50}}]})
    ```
