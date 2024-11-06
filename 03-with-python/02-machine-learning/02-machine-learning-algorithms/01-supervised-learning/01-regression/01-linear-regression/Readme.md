# Linear Regression

## Table Of Contents

# What is Linear Regression?

- **Linear Regression** is a statistical way of measuring the relationship between **variables**.
- There are two types of Linear Regression:

  1. Simple Linear Regression
  2. Multiple Linear Regression

- **Simple linear regression** is used to estimate the relationship between **two quantitative variables**. You can use simple linear regression when you want to know:
  1. How strong the relationship is between two variables (e.g., the relationship between rainfall and soil erosion).
  2. The value of the dependent variable at a certain value of the independent variable (e.g., the amount of soil erosion at a certain level of rainfall).
- Remark:
  - **Multiple Linear Regression** is an extension of **Simple Linear Regression**. It is used when we want to predict the value of a variable based on the value of two or more other variables.
- The most straightforward regression model, featuring one **dependent** and one **independent variable**, is encapsulated by the equation **y = c + m.x**, where:
  1. **y** represents the **predicted** score of the **dependent variable**,
  2. **c** is the constant,
  3. **m** denotes the regression coefficient, and
  4. **x** is the score on the **independent variable**.
- Remarks:
  - The **dependent variable** in a regression analysis may be referred to by various terms, including **outcome variable**, **criterion variable**, **endogenous variable**, or **regressand**
  - **independent variables** are also known as **exogenous variables**, **predictor variables**, or **regressors**

## Linear Regression Line

- The **linear regression line** provides valuable insights into the relationship between the two variables. It represents the best-fitting line that captures the overall trend of how a **dependent variable** (Y) changes in response to variations in an **independent variable** (X).
  1. **Positive Linear Regression Line**: indicates a direct relationship between the **independent variable** (X) and the **dependent variable** (Y). This means that as the value of X increases, the value of Y also increases. The slope of a **positive linear regression line** is **positive**, meaning that the line slants upward from left to right.
  2. **Negative Linear Regression Line**: A indicates an inverse relationship between the **independent variable** (X) and the **dependent variable** (Y). This means that as the value of X increases, the value of Y decreases. The slope of a **negative linear regression line** is **negative**, meaning that the line slants downward from left to right.

## Purpose of Linear Regression

1. **Analysis**: Linear regression can help you understand the relationship between your numerical data i.e how your independent variables correlate to your dependent variable.
2. **Prediction**: After you build your model, you can attempt to predict the output based on a given set of inputs / independent variables.

## Assumptions of simple linear regression

1. Homogeneity of variance (homoscedasticity): the size of the error in our prediction doesn’t change significantly across the values of the independent variable.
2. Independence of observations: the observations in the dataset were collected using statistically valid sampling methods, and there are no hidden relationships among observations.
3. The data follows a normal distribution.
4. The relationship between the independent and dependent variable is linear: the line of best fit through the data points is a straight line (rather than a curve or some sort of grouping factor).

## Disadvantages of Linear Regression

1. **Linear regression** assumes a linear relationship between the **dependent** and **independent variables**. If the relationship is not linear, the model may not perform well.
2. **Linear regression** is sensitive to **multicollinearity**, which occurs when there is a high correlation between **independent variables**. **Multicollinearity** can inflate the **variance of the coefficients** and lead to unstable model predictions.

# Evaluation Metrics for Linear Regression (Loss Functions)

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

# Underfitting and Overfitting Models

- While fitting the model, there can be 2 events which will lead to the bad performance of the model. These events are
  1. Underfitting
  2. Overfitting

## Underfitting

- **Underfitting** is the condition where the model could not fit the data well enough. The under-fitted model leads to low accuracy of the model. Therefore, the model is unable to capture the relationship, trend or pattern in the training data. Underfitting of the model could be avoided by using more data, or by optimizing the parameters of the model.

## Overfitting

- **Overfitting** is the opposite case of underfitting, i.e., when the model predicts very well on training data and is not able to predict well on test data or validation data. The main reason for overfitting could be that the model is memorizing the training dataset. Overfitting can be reduced by doing feature selection.

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
    from yellowbrick.regressor import PredictionError
    from sklearn.datasets import make_regression
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

## Step 4. Model Fit

```py
  X, y = make_regression(
          n_samples=500, n_features=5, noise=50, coef=False
      )
  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
  model = LinearRegression()
  visualizer = PredictionError(model)
  visualizer.fit(X_train, y_train)
  visualizer.score(X_test, y_test)
  visualizer.show()
```

# Regularization Techniques for Linear Models

1. Lasso Regression (L1 Regularization)
2. Ridge Regression (L2 Regularization)
3. Elastic Net Regression

# Example 1:

- **Problem Statement**: Let’s predict the relationship between a person's **BMI** and their **weight**.
- Code:

  ```py
    from sklearn.linear_model import LinearRegression
    import numpy as np

    # Sample data: BMI and corresponding weights
    X = np.array([[18.5], [24.9], [30.0], [35.0], [40.0]])  # BMI
    y = np.array([60, 70, 80, 90, 100])  # Weight in kg

    # Initialize and train the model
    model = LinearRegression()
    model.fit(X, y)

    # Predict weight for a BMI of 28.0
    predicted_weight = model.predict([[28.0]])
    print("Predicted weight for BMI 28.0:", predicted_weight[0], "kg")
  ```

  - Output:

# Resources and Further Reading
