# K-Nearest Neighbors (KNN)

## Table Of Contents

# What is Classification Algorithm?

- A **classification algorithm** takes in a training set of data to create a model that will predict and classify other data into pre-determined categories. For example, an e-commerce company may take in customer data pertaining to sales, location, etc. to determine whether certain customers are likely to stick with the company for the next calendar year.

# What is K-Nearest Neighbors

- **KNN** is a supervised learning algorithm. It is primarily used for **classification** and **regression tasks**.
- KNN is a classification algorithm that measures distances between points. KNN takes a point and measures the k nearest points in the training set. Then, it looks at the labels of each point and classifies the starting point by the majority of the labels surrounding it.

# Steps

## Step 1:

```py
  # Import KNeighborsClassifier
  from sklearn.neighbors import KNeighborsClassifier

  # Instantiate KNeighborsClassifier
  knn = KNeighborsClassifier()
```

```py
  # Fit the classifier
  classifier = knn.fit(scaled_data_train, y_train)
```

```python
  # Predict on the test set
  test_preds = classifier.predict(scaled_data_test)
```

- We create a **knn classifier** object using:
  ```python
     knn = KNeighborsClassifier(n_neighbors=3)
  ```
- The classifier is trained using `X_train` data. The process is termed fitting. We pass the feature matrix and the corresponding response vector.
  ```python
       knn.fit(X_train, y_train)
  ```
- Now, we need to test our classifier on the `X_test` data. knn.predict method is used for this purpose. It returns the predicted response vector, `y_pred`.
  ```python
    y_pred = knn.predict(X_test)
  ```
- Now, we are interested in finding the accuracy of our model by comparing `y_test` and `y_pred`. This is done using the metrics module’s method `accuracy_score`:
  ```py
    print(metrics.accuracy_score(y_test, y_pred))
  ```

# Resources and Further Reading
