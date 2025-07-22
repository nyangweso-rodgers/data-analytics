# scikit-learn

## Table of Contents

# Python Scikit-learn

- [Scikit-learn](https://scikit-learn.org/stable/index.html) was initially developed by David Cournapeau, a data scientist who had worked for Silveregg, a SaaS company, as well as Enthought, a scientific consulting company for years. The sci-kit learn project initially started as scikits.learn, which was a Google summer of code project in 2007.
- **Scikit-learn** is built on top of **NumPy**, **SciPy**, and **Matplotlib**, providing a solid foundation for machine learning.

# Features of Scikit-learn

1. **Supervised learning**: Scikit-learn offers a diverse array of algorithms for supervised learning tasks, including:
   1. **Classification**: Identify which category an object belongs to. Algorithms include:
      1. **Logistic Regression**: Ideal for binary and multiclass classification problems.
      2. **Support Vector Machines** (**SVM**): Effective for high-dimensional spaces.
      3. **K-Nearest Neighbors** (**KNN**): Simple, instance-based learning.
      4. **Decision Trees** and **Random Forests**: Powerful for both **classification** and **regression**.
   2. **Regression**: Predict a continuous value. Algorithms include:
      1. **Linear Regression**: Basic approach for continuous output.
      2. **Ridge and Lasso Regression**: Variants of linear regression with regularization.
      3. **Support Vector Regression** (SVR): Effective for non-linear relationships.
      4. **Gradient Boosting**: Ensemble methods like **XGBoost** and **LightGBM**.
2. **Unsupervised learning**: For tasks where the target values are not known, Scikit-learn provides:
   1. **Clustering**: Group similar data points together. Algorithms include:
      1. K-Means: Simple and efficient for spherical clusters.
      2. DBSCAN: Density-based clustering for arbitrary-shaped clusters.
      3. Hierarchical Clustering: Builds a hierarchy of clusters.
   2. Dimensionality Reduction: Reduce the number of features while preserving information. Techniques include:
      1. Principal Component Analysis (PCA): Linear reduction.
      2. t-Distributed Stochastic Neighbor Embedding (t-SNE): Non-linear reduction.
      3. Linear Discriminant Analysis (LDA): Useful for supervised dimensionality reduction.
3. **Model Selection and Evaluation**: Selecting the right model and evaluating its performance is crucial. Scikit-learn provides tools for:
   1. Cross-validation: Assess the model’s performance by splitting the data into training and testing sets multiple times.
      1. **K-Fold Cross-Validation**: Divides the data into k subsets and uses each one as a test set.
      2. **Leave-One-Out Cross-Validation**: A special case of **k-fold** where k is equal to the number of samples.
   2. Metrics: Evaluate model performance with various metrics such as:
      1. **Accuracy**, **Precision**, **Recall**, **F1 Score** for **classification**.
      2. **Mean Absolute Error** (**MAE**), **Mean Squared Error** (**MSE**), **R² Score** for **regression**.
   3. **Hyperparameter Tuning**: Optimize model parameters using:
      1. **Grid Search**: Exhaustively search through a specified parameter grid.
      2. **Random Search**: Randomly sample parameter combinations.
4. Pipeline and Workflow Management: Scikit-learn's Pipeline class helps streamline the process of building machine learning workflows by chaining multiple processing steps, such as data preprocessing, feature extraction, and model training. This ensures that all steps are applied consistently during both training and evaluation.
   1. **FeatureUnion**: Combine multiple feature extraction methods.
   2. **ColumnTransformer**: Apply different preprocessing steps to different columns of the dataset.
5. **Preprocessing and Feature Engineering**: Scikit-learn provides various tools to preprocess and transform your data:
   1. **Standardization**: Scale features to have **mean zero** and **variance one**.
      1. **StandardScaler**: Standardize features.
      2. **RobustScaler**: Scale features using statistics that are robust to outliers.
   2. **Encoding**: Convert categorical variables into numerical format.
      1. **OneHotEncoder**: One-hot encoding for categorical features.
      2. **LabelEncoder**: Encode labels with values between 0 and n_classes-1.
      3. **Imputation**: Handle missing values.
         1. **SimpleImputer**: Impute missing values with mean, median, or mode.
         2. **KNNImputer**: Use K-Nearest Neighbors to impute missing values.
6. **Feature Selection**: Selecting the most relevant features is key to improving model performance:
   1. **SelectKBest**: Select the top k features based on a statistical test.
   2. **Recursive Feature Elimination** (**RFE**): Recursively remove features and build the model.
7. **Ensemble Methods**: Ensemble methods combine multiple models to improve overall performance:
   1. **Bagging**: Build multiple models from different subsets of the data.
      1. BaggingClassifier,
      2. BaggingRegressor.
   2. Boosting: Sequentially build models to correct errors of previous ones.
      1. AdaBoost,
      2. GradientBoosting,
      3. HistGradientBoosting.
   3. Voting: Combine predictions from multiple models.
      1. VotingClassifier,
      2. VotingRegressor.

# Install Scikit Learn

- Scikit assumes you have a running Python 2.7 or above platform with NumPY (1.8.2 and above) and SciPY (0.13.3 and above) packages on your device. Once we have these packages installed we can proceed with the installation.
- For `pip` installation,
  ```sh
    pip install scikit-learn
  ```

# Using Scikit-Learn

- Once you are done with the installation, you can use scikit-learn easily in your Python code by importing it as:
  ```py
    import sklearn
  ```

# Scikit Learn Dataset

- **Scikit-learn** provides several datasets suitable for learning and testing your models. These are mostly well-known datasets. They are large enough to provide a sufficient amount of data for testing models, but also small enough to enable acceptable training duration.
- Examples:
  1. The function `sklearn.datasets.load_boston()` returns the data about the prices of houses in the Boston area (the prices aren’t updated!). There are 506 observations, while the input matrix has 13 columns (features):
     ```py
      from sklearn.datasets import load_boston
      x, y = load_boston(return_X_y=True)
      x.shape, y.shape # Output:
     ```
     - This dataset is suitable for **multi-variate regression**.
  2. The other example is the dataset related to wine. It can be obtained with the function `sklearn.datasets.load_wine()`:
     ```py
      from sklearn.datasets import load_wine
      x, y = load_wine(return_X_y=True)
      x.shape, y.shape
      np.unique(y)
     ```
     - This dataset is suitable for **classification**. It contains 13 features related to three different wine cultivators from Italy. There are 178 observations.
  3. **Iris Databaset**: It is a dataset of a flower, it contains 150 observations about different measurements of the flower.
     - The data set contains information of 3 classes of the iris plant with the following attributes::
       - sepal length
       - sepal width
       - petal length
       - petal width
       - iris setosa
       - iris Versicolour
       - Iris Virginica
     - Access the dataset:
       ```py
         from sklearn.datasets import load_iris
         iris_df = load_iris()
       ```
     - or:
       ```python
         from sklearn import datasets
         # Load data
         iris_df = datasets.load_iris()
         # Print shape of data to confirm data is loaded
         print(iris_df.data.shape)
       ```
     - check the features and labels
       ```py
        # Check out the features and labels
        print("Features:", iris.feature_names)
        print("Labels:", iris.target_names)
       ```
     - Display the first 5 records:
       ```py
        # Display the first 5 records
        print("First 5 records:\n", iris.data[:5])
       ```

# Generating Random Dummy Data

- **Dummy data** refers to datasets that do not contain useful data. Instead, they reserve space where real or useful data should be present. **Dummy data** is a **placeholder for testing**, so it must be evaluated carefully to prevent unintended results.
- **Sklearn** makes it easy to generate reliable dummy data. We simply use the functions `make_classification()` for **classification** data or `make_regression()` for **regression** data. You’ll also want to set the parameters, like the number of samples and features.
- Example:
  - 1000 samples and 20 features
    ```py
      from sklearn.datasets import make_classification
      X, y = make_classification(n_samples=1000, n_features=20)
    ```

# Splitting Dataset

- Before training a model, it’s important to split your data into **training** and **testing sets**. This helps you evaluate the performance of your model on unseen data.
- Example:

  - For `iris_df`:

    ```py
      from sklearn.model_selection import train_test_split

      # Split the data (80% training, 20% testing)
      X_train, X_test, y_train, y_test = train_test_split(iris.data, iris.target, test_size=0.2, random_state=42)

      print("Training set size:", len(X_train))
      print("Testing set size:", len(X_test))
    ```

# Data Preprocessing

- Computers are generally bad at understanding text, but they are very good with numbers. So we often need to transform our text data into numbers so that our model can better understand it.
- You often need to transform data in such a way that the **mean of each column** (**feature**) is **zero** and the **standard deviation** is **one**. You can apply class `sklearn.preprocessing.StandardScaler` to do this:

## Handling Missing Values with `sckit-learn`

- When a dataset has missing values, many problems in an ML algorithm can occur. In each column, we need to identify and replace missing values before we model prediction tasks. This process is called **data imputation**.
- To get the status of missing values, run:
  ```py
    df.isna().sum()
  ```
- The following are the usual approaches to handling missing data:
  1. By dropping columns containing `NaNs`.
  2. By dropping rows containing `NaNs`.
  3. By imputing the missing values suitably.

### 1. Handling Missing Vlues - Removing Missing Values

- The quickest way to remove missing values completely is done as follows:
  ```py
    df = df.dropna()
  ```
- If you wanted to be specific on removing **rows** or **columns**, you can specify it in the axis parameter, as follows.
- Example (The entire column with missing values will be removed)
  ```py
    df = df.dropna(axis='columns')
  ```

### 2. Handling Missing Vlues - Filling The Missing Values

- With Pandas, filling the missing values is very straightforward.
- **Example** (fill missing values with a given number):
  ```py
    df = df.fillna(2)
  ```
- Remark:
  - You can also use `ffill` (**forward fill**) or `bfill`(**backward fill)**, where you fill the values preceding or following the missing value.
- **Example** (Forward Fill Missing Value):
  ```py
    df = df.fillna(method="ffill")
  ```
- **Example** (Backward Fill Missing Value):
  ```py
    df = df.fillna(method="bfill")
  ```

### 3. Handling Missing Vlues - Imputing Missing Values With Iterative Imputer

- It’s easy to stick with traditional methods for imputing missing values, like **mode** (for **classification**) or the **mean/median** (for **regression**). But **Sklearn** provides more powerful, simpler ways to impute missing values.
- In **Sklearn**, the `IterativeImputer` class allows us to use an entire set of features to locate and eliminate missing values. In fact, it is specifically designed to estimate missing values by taking them as a function of other features.
- This approach repeatedly defines a model to predict missing features as a function of other features. This improves our dataset with each iteration.
- To use this built-in iterative imputation feature, you must import `enable_iterative_imputer`, since it is still in the experimental phase.
  ```py
    # explicitly require this experimental feature
    from sklearn.experimental import enable_iterative_imputer
    # now you can import normally from sklearn.impute
    from sklearn.impute import IterativeImputer
  ```
- **Example**:
  - With this code, any missing values in a dataframe will be filled in a new dataframe called `impute_df`
  - We set the number of iterations and verbosity, which is optional. The imputer is fitted to the dataset that has missing values, generating our new dataframe.
    ```py
      from sklearn.experimental import enable_iterative_imputer
      from sklearn.impute import IterativeImputer
      df = pd.DataFrame(*a dataset with missing values that we want to impute*)
      imp = IterativeImputer(max_iter=10, verbose=0)
      imp.fit(df)
      impute_df = imp.transform(df)
      impute_df = pd.DataFrame(impute_df, columns=df.columns)
    ```
- **Example**:

  - Let's create a pandas DataFrame X with one missing value.

    ```python
      import numpy as np
      import pandas as pd

      X = pd.DataFrame({'Age':[20, 30, 10, np.nan, 10]})
    ```

  - Now, we shall import the `SimpleImputer` from `scikit-learn`:

    ```py
     from sklearn.impute import SimpleImputer
    ```

  - We shall now instantiate a `SimpleImputer` that by default does **mean imputation**, by replacing all missing values with the average of the other values present. The missing value is calculated as (20+30+10+10)/4=17.5. Let's verify the output.
    ```py
      # Mean Imputation
      imputer = SimpleImputer()
      imputer.fit_transform(X) # Output: array([[20. ],[30. ],[10. ],[17.5],[10. ]])
    ```

- **Remarks**:

  - When imputing missing values, if we would like to preserve information about which values were missing and would like to use that as a feature, then we can do it by setting the `add_indicator` attribute in **scikit-learn’s** `SimpleImputer` to `True`
  - In order to encode the missingness of values as a feature, we can set the `add_indicator` argument to `True` and observe the output.
    ```py
      # impute the mean and add an indicator matrix (new in scikit-learn 0.21)
      imputer = SimpleImputer(add_indicator=True)
      imputer.fit_transform(X)
    ```
  - In the output, we will observe that the indicator value of 1 is inserted at index 3 where the original data was missing. This feature is new in scikit-learn version 0.21 and above. In the next section, we shall see how we can use the HistGradientBoosting Classifier that natively handles missing values.

## Data Preporocessing 4. Using `HistGradientBoosting` Classifier

- To use this new feature in scikit-learn version 0.22 and above.
- Example:
  - Download the very popular Titanic-Machine learning from Disaster dataset from kaggle.
    ```py
      import pandas as pd
      train = pd.read_csv('http://bit.ly/kaggletrain')
      test = pd.read_csv('http://bit.ly/kaggletest', nrows=175)
    ```
  - Create the datasets for training and testing.
    ```py
      train = train[['Survived', 'Age', 'Fare', 'Pclass']]
      test = test[['Age', 'Fare', 'Pclass']]
    ```
  - To better understand the missing values, let’s compute the number of missing values in each column of the training and test sets.
    ```python
      # count the number of NaNs in each column
      print(train.isna().sum())
      print(test.isna().sum())
    ```
  - We'll see that both train and test subsets contain missing values. Let the output label for the classifier be `Survived` indicated by `1` if the passenger survived and `0` if the passenger did not.
    ```py
      label = train.pop('Survived')
    ```
  - import `HistGradientBoostingClassifier` from `scikit-learn`
    ```python
       from sklearn.experimental import enable_hist_gradient_boosting
       from sklearn.ensemble import HistGradientBoostingClassifier
    ```
  - As always, let us instantiate the classifier, fit on the training set train and predict on the test set test . Note that we did not impute the missing values; Ideally, when there are missing values NaN, we do get errors. Let us check what happens now.
    ```py
      clf = HistGradientBoostingClassifier()
      # no errors, despite NaNs in train and test sets!
      clf.fit(train, label)
      clf.predict(test)
    ```
  - Surprisingly, there are no errors and we get predictions for all records in the test set even though there were missing values.

## Data Preporocessing 1. Label Encoder

- This is a encoder provided by scikit that transforms categorical data from text to number. If we have n possible values in our dataset, then `LabelEncoder` model will transform it into numbers from 0 to n-1 so that each textual value has a number representation.
- Examples 1.
  ```py
   from sklearn import preprocessing
   weather = ['Clear', 'Clear', 'Clear', 'Clear', 'Clear', 'Clear',
               'Rainy', 'Rainy', 'Rainy', 'Rainy', 'Rainy', 'Rainy',
               'Snowy', 'Snowy', 'Snowy', 'Snowy', 'Snowy', 'Snowy']
   labelEncoder = preprocessing.LabelEncoder();
   print (labelEncoder.fit_transform(weather)) # Output: [0 0 0 0 0 0 1 1 1 1 1 1 2 2 2 2 2 2]
  ```

## Dimensionality Reduction

- **Dimensionality reduction** involves the selection or extraction of the most important components (**features**) of a multidimensional dataset. **Scikit-learn** offers several approaches to dimensionality reduction. One of them is the **principal component analysis** or **PCA**.

## Model Selection

- When training and testing machine learning models, you need to split your datasets randomly into **training** and **tests sets**. This includes both the inputs and their corresponding outputs. The function `sklearn.model_selection.train_test_split()` is useful in such cases:
- First, import the data:
  ```py
   import numpy as np
   from sklearn.model_selection import train_test_split
   x, y = np.arange(1, 21).reshape(-1, 2), np.arange(3, 40, 4)
   print(x, y)
  ```
- Secondly, split the dataset:
  ```python
   x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.4, random_state=0)
   print(x_train, y_train, y_test)
  ```

# Evaluation

## 1. Model Evaluate Using `accuracy`

- You can evaluate model performance using `accuracy`, one of the simplest metrics.
- Example:

  ```py
      from sklearn.metrics import accuracy_score

      # Calculate the accuracy
      accuracy = accuracy_score(y_test, predictions)
      print("Model Accuracy:", accuracy)
  ```

# Using Pickle For Model Persistence

- Model Persistence allows us to reuse an ML model without retraining it. Sklearn’s **Pickle** model allows us to reuse a model and achieve model persistence. Once you save a model as a Pickle, it can be easily loaded later at any time for more predictions.
- To serialize your algorithms and save them, you can use either the `pickle` or `joblib` Python libraries.
- Example:
  - Take a look at the code example below.
    ```python
      import pickle
      # Save a KNN model
      saved_model = pickle.dumps(knn)
      # Load a saved KNN model
      load_model = pickle.loads(saved_model)
      # Make new predictions from a pickled model
      load_model.predict(test_X)
    ```

# Plotting a Confusion Matrix

- A **confusion matrix** is a table that describes a classifier’s performance for test data.
- Sklearn’s most recent release adds the function `plot_confusion_matrix` to generate an accessible and customizable **confusion matrix** that maps our true positive, false positive, false negative, and true negative values.
- Example:
  - Take a look at this code example from the Sklearn documentation.
    ```python
      from sklearn.metrics import confusion_matrix
      y_true = [2, 0, 2, 2, 0, 1]
      y_pred = [0, 0, 2, 2, 0, 2]
      confusion_matrix(y_true, y_pred)
    ```

# Creating Visualization For Decision Trees

- We can now visualize decision trees with matplotlib using `tree.plot_tree`. This means you don’t have to install any dependencies to create simple visualizations. You can then save your tree as a `.png` file for easy access.
- Example:
  - Take a look at this example from the Sklearn documentation. The example visual decision tree should give you the basic structure of what Scikit-learn generates (see the official documentation for further details).
    ```py
      tree.plot_tree(clf)
    ```

# Resources and Further Reading
