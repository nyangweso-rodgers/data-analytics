# Linear Regression

## Table Of Contents

# Introduction To Linear Regression
* __Linear regression__ is a __supervised machine learning algorithm__ that is used to predict a _continuous target variable_. 

* For __simple linear regression__, where there is _one independent variable_ (feature) and _one dependent variable_ (target) the algorithm can be represented by the following equation.

    __y = a + bX__

    where y = __dependent variable__, X = __explanatory variable__, b = __slope__ of the line and a = __intercept__

* __Simple linear regression__ can be visualised as a scatter plot where the x-axis contains the dependent variable and the y-axis contains the independent variable. The linear regression algorithm draws a line of best fit through the data points minimising the variation between the predicted and actual outputs.

# When To Use a Linear Regression?
* Linear Regression can only be used to solve regression-based problems.
* There must be a linear relationship between the dependent and independent variables.
* Residuals must form a normal distribution.
* There must be no correlation between features.
* The algorithm assumes that training data is randomly sampled.
* Best suited to regression-based problems where the relationships in the data are both linear and simple.

# Advantages Of Linear Regression
* Highly interpretable and fast to train.
* Performs very well on linearly separable data.

# Disadvantages Of Linear Regression
* Not robust to outliers.
* Very simplistic and so it doesn't model the complexities found in real-world data well.
* This algorithm is also prone to overfitting.