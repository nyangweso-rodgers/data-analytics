# Logistics Regression

# Introduction to Logistics Regression

- **Logistic regression** is essentially **linear regression** moulded to fit a classification problem. Instead of fitting a straight line, **logistic regression** applies the logistic function to squeeze the output of a linear equation between 0 and 1. The result is an **S-shaped curve** rather than a straight line through the data points.
- A threshold between 0 and 1 is chosen to separate the classes, typically this is 0.5. In essence, we draw a horizontal line across the S curve at 0.5. Any data points above this line belong to class 1 and any below to class 0.

# When To Use Logistics Regression?

1. This algorithm can only be used to solve classification problems.
2. There must be a linear relationship between features and the target variable.
3. The number of observations must be larger than the number of features.
4. Best suited to classification problems where the relationships in the data are both linear and simple.

# Steps

## Step.1

```py
    import seaborn as sns
    from sklearn.datasets import make_classification
    from sklearn.linear_model import LogisticRegression

    X, y = make_classification(n_features=1,
                            n_informative=1,
                            n_redundant=0,
                            n_classes=2,
                            n_clusters_per_class=1,
                            flip_y=0.2,
                            random_state=0)
    df = pd.DataFrame(X, columns=['x'])
    df['y'] = y

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    lr = LogisticRegression(C=1e5)

    model = lr.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    sns.regplot(x='x', y='y', data=df, logistic=True, ci=None)
```
