# Introduction To Random Forest

- The **random forest** algorithm builds a ‘forest’ of **decision trees**. Each tree in the forest generates a prediction from a given set of features. Once all predictions have been generated a majority vote is taken and the most commonly predicted class forms the final prediction.

# Steps For Random Forests Algorithm

1. The training dataset is randomly split into multiple samples based on the number of trees in the forest. The number of trees is set via a hyperparameter.
2. The decision trees are trained in parallel using one of the data subsets.
3. The output of all trees is evaluated and the most commonly occurring prediction is taken as the final result.

# When To Use Random Forest?

- This algorithm can be used to solve both **classification** and **regression-based **problems.
- It is particularly well suited to large datasets with high dimensionality as the algorithm inherently performs feature selection.

# Advantages Of Random Forest

- It can model both linear and non-linear relationships.
- It is not sensitive to outliers.
- Random Forest is able to perform well on datasets containing missing data.

# Disadvantages Of Random Forest

- Random Forest can be prone to overfitting although this can be mitigated to some degree with **pruning**.
- It is not as interpretable as linear and logistic regression although it is possible to extract feature importances to give some level of interpretability.

# Steps

## Step 1:

- We usually begin our regression journey by importing the packages, classes, and functions we need:
  ```py
    import numpy as np
    from sklearn.datasets import load_boston
    from sklearn.ensemble import RandomForestRegressor
    from sklearn.model_selection import train_test_split
  ```

## Step 2:

- The next step is to get the data to work with and split the set into the training and test subsets.
- Examples:
  - For Boston dataset:
    ```py
        x, y = load_boston(return_X_y=True)
        x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.33, random_state=0)
    ```

## Step 3:

- Now, we need to create our regressor and fit (train) it with the subset of data chosen for training:
  ```python
    regressor = RandomForestRegressor(n_estimators=10, random_state=0)
    regressor.fit(x_train, y_train)
    RandomForestRegressor(bootstrap=True, criterion='mse', max_depth=None,
        max_features='auto', max_leaf_nodes=None,
        min_impurity_decrease=0.0, min_impurity_split=None,
        min_samples_leaf=1, min_samples_split=2,
        min_weight_fraction_leaf=0.0, n_estimators=10,
        n_jobs=None, oob_score=False, random_state=0, verbose=0,
        warm_start=False)
  ```

## Step 4:

- Once the model is trained, we check its score (the **coefficient of determination**) on the training set, and what’s more important on the test set, i.e., with the data not used to fit the model:
  ```python
    regressor.score(x_train, y_train)
    regressor.score(x_test, y_test)
  ```

# Resources and Further Reading
