# Machine Learning Algorithms

## Table Of Contents

- [Introduction To ML Algorithms](#Introduction-To-ML-Algorithms)

- [Machine Learning Algorithms]()

  - [Linear Regression]()

    - [Theory](https://github.com/nyangweso-rodgers/Data_Analytics/tree/main/Analytics_with_Python/Machine_Learning_Algorithms/Linear_Regression)

  - [Logistic Regression]()

    - [Theory](https://github.com/nyangweso-rodgers/Data_Analytics/tree/main/Analytics_with_Python/Machine_Learning_Algorithms/Logistics_Regression)

  - [Random Forest]()

    - [Theory](https://github.com/nyangweso-rodgers/Data_Analytics/tree/main/Analytics_with_Python/Machine_Learning_Algorithms/Random_Forest)

  - [XGBoost]()

    - [Theory](https://github.com/nyangweso-rodgers/Data_Analytics/tree/main/Analytics_with_Python/Machine_Learning_Algorithms/XGBoostcd mac)

  - [K-means]()
    - [Theory](https://github.com/nyangweso-rodgers/Data_Analytics/tree/main/Analytics_with_Python/Machine_Learning_Algorithms/K_Means)

- [Further Reading]()
  - [Deploy Your Machine Learning Model as a REST API](https://towardsdatascience.com/deploy-your-machine-learning-model-as-a-rest-api-4fe96bf8ddcc)
  - [Considerations for Deploying Machine Learning Models in Production](https://towardsdatascience.com/considerations-for-deploying-machine-learning-models-in-production-89d38d96cc23)

# Machine Learning Concepts

## 1. Algorithm

- An **algorithm** is a set of rules a computer uses to solve a problem. It finds patterns in data and makes predictions.
- There are several types of algorithms in machine learning:
  1. **Supervised Learning**: Learn from labelled examples to predict or classify new data.
  2. **Unsupervised Learning**: Discover patterns in data without labels.
  3. **Reinforcement Learning**: Make decisions by taking actions in an environment
- The choice of ML Algorithm will mostly depend on things such as the _size_, _dimensionality_, and _sparsity_ of the data. The target variable, the quality of the data and the interactions and statistical relationships that exist both within features and between the features and target variable.

## 2. Model

- A **model** is created by training an algorithm with data. It finds the patterns and relationships found in the data. This lets the model predict new data.
- For example:
  1. **Linear Regression Model**: Predicts values by fitting a line to the data.
  2. **Decision Tree Model**: Makes predictions by splitting data into groups based on features.
  3. **Support Vector Machine (SVM) Model**: Finds the best boundary to separate different categories.

## 3. Features

- **Features** are input data used to make predictions. They are measurable properties or characteristics of the data. They can be numerical or categorical.
  For example, consider a model that predicts house prices. _Features_ could be the size, location, and age of the house. Each **feature** helps the **model** understand how these aspects influence the price.

## 4. Labels

- **Labels** are the outcomes that a **machine learning model** tries to predict. Each set of features is paired with a label in supervised learning. Similar to features, they can be numerical or categorical.
  Consider a model that classifies emails as “spam” or “not spam”. The label is either “spam” or “not spam.” The model learns patterns from these features to predict the label for new emails.

## 5. Underfitting

- **Underfitting** happens when a machine learning model is too simple to understand the data patterns. As a result, it performs poorly on both training data and new data. This usually occurs if the model lacks complexity or hasn’t been trained long enough. Increase the model’s complexity or add more features to fit underfitting.

## 6. Overfitting

- **Overfitting** happens when a machine learning model learns the training data too well, including noise and outliers. This makes the model perform well on training data but poorly on new data. This occurs because the model is too complex and memorizes the training data rather than generalizes it. To prevent **overfitting**, techniques like **cross-validation**, **pruning**, and **regularization** are used.

## 7. Hyperparameters

- **Hyperparameters** are settings that guide the learning process and the model’s structure. They are chosen before training starts. In contrast, **parameters** are learned from the data during training,
- Common **hyperparameters** include:
  1. **Learning Rate**: Controls how much the model’s weights are updated during each training step.
  2. **Number of Hidden Layers**: Specifies the number of layers between the input and output layers in the network.
  3. **Batch Size**: Defines how many training examples are used in each iteration.
  4. **Number of Epochs**: Determines how many times the entire training dataset is passed through the model.

# How Use FastAPI for Machine Learning

## What is FastAPI?

- **FastAPI** provides a quick way to build a backend service with Python. With a few decorators, you can turn your Python function into an API application.

## Why use FastAPI for machine learning?

- **FastAPI** enables data scientists to easily create APIs for the following projects:
  1. Deploying prediction models: The data science team may have trained a model for the prediction of the sales demand in a warehouse. To make it useful, they have to provide an API interface so other parts of the stock management system can use this new prediction functionality.
  2. Suggestion engines: One of the very common uses of machine learning is as a system that provides suggestions based on the users’ choices. For example, if someone puts certain products in their shopping cart, more items can be suggested to that user. Such an e-commerce system requires an API call to the suggestion engine that takes input parameters.
  3. Dynamic dashboards and reporting systems: Sometimes, reports for data science projects need to be presented as dashboards so users can inspect the results themselves. One possible approach is to have the data model provide an API. Frontend developers can use this API to create applications that allow users to interact with the data.

## Advantages of using FastAPI

1. Fast development
2. Fast documentation: FastAPI provides automatic interactive API documentation using [Swagger UI](), which is an industry standard. No extra effort is required to build clear documentation with API call examples. This creates an advantage for busy data science teams who may not have the energy and expertise to write technical specifications and documentation.
3. Easy testing: Testing FastAPI is made simple thanks to [Starlette](https://www.starlette.io/testclient/) and [HTTPX](https://www.python-httpx.org/)
4. Fast deployment: FastAPI comes with a [CLI tool](https://fastapi.tiangolo.com/fastapi-cli/) that can bridge development and deployment smoothly. It allows you to switch between development mode and production mode easily. Once development is completed, the code can be easily deployed using a [Docker container](https://fastapi.tiangolo.com/deployment/docker/) with images that have Python prebuilt.

## How to use FastAPI for a machine learning project

### Step 1. Start a FastAPI Project

### Step 2.Setup Environment Dependencies

### Step 3. Set up your machine learning model

- Arrange your machine learning code in the `main.py` file. We will start with a script that trains our model:
  ```py
    import pandas as pd
    from sklearn.model_selection import train_test_split
    from sklearn import preprocessing
    from sklearn.neighbors import KNeighborsClassifier
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
  ```

### Step 4. Request a Response

### Step 5. Request with query parameters

# Resources and Further Reading

1. [machinelearningmastery.com -7-key-terms-every-machine-learning-beginner-should-know](https://machinelearningmastery.com/7-key-terms-every-machine-learning-beginner-should-know/?ref=dailydev)
2. [jetbrains blog - How to Use FastAPI for Machine Learning](https://blog.jetbrains.com/pycharm/2024/09/how-to-use-fastapi-for-machine-learning/?ref=dailydev)
