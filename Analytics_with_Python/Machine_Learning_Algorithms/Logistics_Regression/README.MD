# Logistics Regression

## Table Of Contents
1. [Introduction to Logistics Regression](#Introduction-to-Logistics-Regression)
2. [When To Use Logistics Regression?](#When-To-Use-Logistics-Regression?)
3. [Advantages Of Logistics Regression](#Advantages-Of-Logistics-Regression)
4. [Disadvantages Of Logistics Regression](#Disadvantages-Of-Logistics-Regression)

# Introduction to Logistics Regression
* __Logistic regression__ is essentially __linear regression__ moulded to fit a __classification__ problem. Instead of fitting a straight line, __logistic regression__ applies the [logistic function](https://en.wikipedia.org/wiki/Logistic_function) to squeeze the output of a linear equation between 0 and 1. The result is an __S-shaped curve__ rather than a straight line through the data points.

* A threshold between 0 and 1 is chosen to separate the classes, typically this is 0.5. In essence, we draw a horizontal line across the S curve at 0.5. Any data points above this line belong to class 1 and any below to class 0.

# When To Use Logistics Regression?
* This algorithm can only be used to solve __classification problems__.
* There must be a linear relationship between features and the target variable.
* The number of observations must be larger than the number of features.
* Best suited to classification problems where the relationships in the data are both linear and simple.

# Advantages Of Logistics Regression
* As with linear regression, this algorithm is highly interpretable and fast to train.
* It performs very well on linearly separable data.

# Disadvantages Of Logistics Regression
* Prone to overfitting.
* As with Linear Regression, it does not model complex relationships well.