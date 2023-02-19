# Test 1: Codility

# Description
* You are given a table __flats__ with the following structure:

    ```sql
        create table flats (
            id integer primary key,
            city varchar(40) not null,
            price integer not null
        );
    ```

* Each of row of table __flats__ represents a flat located in a city available for sale for a give price.

* Wrire an sql query that for each city finds the three cheapest flats located in the city. In case of a tie, the query may return any three flats with the cheapest prices.

* The result table should contain three columns: id, city and price and should be sorted by id column. If the city has less than three flats for sale, the result table should contain all of them.

* Please use an OVER clause to simlify your solution.

* Example: Given

    id | city | price
    |---------|-------------|------|
    25 | London | 200000
    5 | Cairo | 90000
    7 | London | 200000
    18 | Warsaw | 150000
    2 | London | 178000
    3 | Cariro | 300000
    29 | London | 500000
    9 | London | 200000

* One of the possible output is

    id | city | price
    |---------|-------------|------|
    2 | London | 178000
    3 | Cairo | 300000
    5 | Cairo | 90000
    7 | London | 200000
    9 | London | 200000
    18 | Warsaw | 150000



