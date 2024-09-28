# Linear Regression

## Table Of Contents

# What is Linear Regression?

- The most straightforward regression model, featuring one **dependent** and one **independent variable**, is encapsulated by the equation **y = c + m.x**, where:
  1. **y** represents the **predicted** score of the **dependent variable**,
  2. **c** is the constant,
  3. **m** denotes the regression coefficient, and
  4. **x** is the score on the independent variable.
- Remarks:
  - The **dependent variable** in a regression analysis may be referred to by various terms, including **outcome variable**, **criterion variable**, **endogenous variable**, or **regressand**
  - **independent variables** are also known as **exogenous variables**, **predictor variables**, or **regressors**

# Disadvantages of Linear Regression

1. **Linear regression** assumes a linear relationship between the **dependent** and **independent variables**. If the relationship is not linear, the model may not perform well.
2. **Linear regression** is sensitive to **multicollinearity**, which occurs when there is a high correlation between **independent variables**. **Multicollinearity** can inflate the **variance of the coefficients** and lead to unstable model predictions.

# Types of Linear Regression

1. Simple linear regression
2. Multiple linear regression
3. Logistic regression
4. Ordinal regression
5. Multinomial regression

## 1. Simple Linear Regression

- **Simple linear regression** is used to estimate the relationship between **two quantitative variables**. You can use simple linear regression when you want to know:
  1. How strong the relationship is between two variables (e.g., the relationship between rainfall and soil erosion).
  2. The value of the dependent variable at a certain value of the independent variable (e.g., the amount of soil erosion at a certain level of rainfall).
- **Assumptions of simple linear regression**:
  1. Homogeneity of variance (homoscedasticity): the size of the error in our prediction doesn’t change significantly across the values of the independent variable.
  2. Independence of observations: the observations in the dataset were collected using statistically valid sampling methods, and there are no hidden relationships among observations.
  3. The data follows a normal distribution.
  4. The relationship between the independent and dependent variable is linear: the line of best fit through the data points is a straight line (rather than a curve or some sort of grouping factor).

# Linear Regression Line

- The **linear regression line** provides valuable insights into the relationship between the two variables. It represents the best-fitting line that captures the overall trend of how a **dependent variable** (Y) changes in response to variations in an **independent variable** (X).
  1. **Positive Linear Regression Line**: indicates a direct relationship between the **independent variable** (X) and the **dependent variable** (Y). This means that as the value of X increases, the value of Y also increases. The slope of a **positive linear regression line** is **positive**, meaning that the line slants upward from left to right.
  2. **Negative Linear Regression Line**: A indicates an inverse relationship between the **independent variable** (X) and the **dependent variable** (Y). This means that as the value of X increases, the value of Y decreases. The slope of a **negative linear regression line** is **negative**, meaning that the line slants downward from left to right.

# Evaluation Metrics for Linear Regression

- A variety of [evaluation measures]() can be used to determine the strength of any linear regression model. These assessment metrics often give an indication of how well the model is producing the observed outputs.
- The most common measurements are:
  1. Mean Square Error (MSE)
  2. Mean Absolute Error(MAE)
  3. Root Mean Squared Error (RMSE)
  4. Coefficient of Determination (R-squared)
  5. Adjusted R-Squared Error

## 1. Mean Square Error (MSE)

- **MSE** is an evaluation metric that calculates the average of the squared differences between the actual and predicted values for all the data points. The difference is squared to ensure that negative and positive differences don’t cancel each other out.
- **MSE** is a way to quantify the accuracy of a model’s predictions. **MSE** is sensitive to outliers as large errors contribute significantly to the overall score.

## 2. Mean Absolute Error(MAE)

- **MAE** is an evaluation metric used to calculate the accuracy of a regression model. **MAE** measures the average absolute difference between the predicted values and actual values.
- Lower **MAE** value indicates better model performance. It is not sensitive to the outliers as we consider absolute differences.

## 3. Root Mean Squared Error (RMSE)

- **RMSE** is the square root of the residual's variance. It describes how well the observed data points match the expected values, or the model’s absolute fit to the data.

## 4. Coefficient of Determination (R-squared)

- **R-Squared** is a statistic that indicates how much variation the developed model can explain or capture. It is always in the range of 0 to 1. In general, the better the model matches the data, the greater the **R-squared** number.
- **R squared** metric is a measure of the proportion of variance in the dependent variable that is explained the independent variables in the model.

## 5. Adjusted R-Squared Error

- **Adjusted R2** measures the proportion of variance in the dependent variable that is explained by independent variables in a regression model. **Adjusted R-square** accounts the number of predictors in the model and penalizes the model for including irrelevant predictors that don’t contribute significantly to explain the variance in the dependent variables.

# Python Implementation of Linear Regression

## Step 1. Import the necessary libraries

```py
    # Import libraries
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import seaborn as sns
    from sklearn.model_selection import train_test_split
    from pandas.core.common import random_state
    from sklearn.linear_model import LinearRegression
```

## Step 2. Load Dataset

```py
    df = pd.read_csv("data.csv")
```

## Step 3. Data Analysis

- We can also find how the data is distributed visually using Seaborn distplot
  ```py
    # Data distribution
    plt.title('Salary Distribution Plot')
    sns.distplot(df_sal['Salary'])
    plt.show()
  ```

# Regularization Techniques for Linear Models

1. Lasso Regression (L1 Regularization)
2. Ridge Regression (L2 Regularization)
3. Elastic Net Regression

# Resources and Further Reading
