# XGBoost

## Table Of Contents
1. [Introduction To XGBoost](#Introduction-To-XGBoost)
2. [When To Use XGBoost?](#When-To-Use-XGBoost?)
3. [Advantages Of XGBoost](#Advantages-Of-XGBoost)
4. [Disadvantages Of XGBoost](#Disadvantages-Of-XGBoost)

# Introduction To XGBoost
* __XGBoost__ is an algorithm based on __gradient-boosted decision trees__. It is similar to Random Forest in that it builds an ensemble of decision trees but rather than training the models in parallel, XGBoost trains the models sequentially. Each decision tree learns from the errors produced by the previous model. This technique of training models sequentially is known as __boosting__.

* The gradient in __XGBoost__ refers to a specific type of boosting where __weak learners__ are used. __Weak learners__ are very simple models that only just perform better than random chance. The algorithm starts with an initial weak learner. Each subsequent model targets the errors produced by the previous decision tree. This continues until no further improvement can be made and results in a final strong learner model.

# When To Use XGBoost?
* It can be used to solve both classification and regression-based problems.
* XGBoost is generally considered one of the best and most flexible algorithms for supervised learning on structured data and is therefore suited to a wide range of datasets and problem types.

# Advantages Of XGBoost
* XGboost is highly flexible in that it works equally well on small and large datasets.
* It is computationally efficient and therefore faster to train models compared to other complex algorithms.

# Disadvantages Of XGBoost
* It does not work as well on very sparse or unstructured data.
* It is considered a black box model and is less interpretable than some other algorithms.
* XGBoost can be sensitive to outliers due to the mechanism of models learning from the errors of their predecessors.
