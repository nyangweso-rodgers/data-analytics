# Random Forests

## Table Of Contents
1. [Introduction To Random Forest](#Introduction-To-Random-Forest)
2. [Steps For Random Forests Algorithm](#Steps-For-Random-Forests-Algorithm)
3. [When To Use Random Forest?](#When-To-Use-Random-Forest?)
4. [Advantages Of Random Forest](#Advantages-Of-Random-Forest)
5. [Disadvantages Of Random Forest](#Disadvantages-Of-Random-Forest)

# Introduction To Random Forest
* The __random forest__ algorithm builds a ‘forest’ of __decision trees__. Each tree in the forest generates a prediction from a given set of features. Once all predictions have been generated a majority vote is taken and the most commonly predicted class forms the final prediction.

* A __decision tree__ is a very intuitive algorithm. It has a flowchart-like structure containing a series of nodes representing a test. The outcome of each test results in a split and one or more leaf nodes are created until the final prediction is achieved. _Hyperparameters_ determine how deep the tree grows and the node-splitting function used.

# Steps For Random Forests Algorithm
1. The training dataset is randomly split into multiple samples based on the number of trees in the forest. The number of trees is set via a hyperparameter.
2. The decision trees are trained in parallel using one of the data subsets.
3. The output of all trees is evaluated and the most commonly occurring prediction is taken as the final result.

# When To Use Random Forest?
* This algorithm can be used to solve both classification and regression-based problems.
* It is particularly well suited to large datasets with high dimensionality as the algorithm inherently performs feature selection.

# Advantages Of Random Forest
* It can model both linear and non-linear relationships.
* It is not sensitive to outliers.
* Random Forest is able to perform well on datasets containing missing data.

# Disadvantages Of Random Forest
* Random Forest can be prone to overfitting although this can be mitigated to some degree with __pruning__.
* It is not as interpretable as linear and logistic regression although it is possible to extract feature importances to give some level of interpretability.