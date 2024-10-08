# Decision Tree

# Introduction to Decision Tree

- **Decision Tree** analysis is a general, predictive modelling tool constructed via an algorithmic approach that identifies ways to split a data set based on different conditions. **Decision Trees** are **non-parametric Supervised** learning method used for both **classification** and **regression** tasks. The goal is to _create a model that predicts the value of a target variable by learning simple decision rules inferrred from the data features_.

# Definitions of Terms

- **Instances**: refer to the vector of feature that define the input space.
- **Attribute**: a quantity describing an instance
- **Concept**: the function that maps input to output
- **Target Concept**: the function that we are trying to find i.e., the actual answer
- **Hypothesis Class**: set of all the possible functions.
- **Sample**: set of inputs paired with a label, which is the correct output (also known as the **Training Set**)
- **Candidate Concept**: a concept which we think is a target concept
- **Testing Concet**: similar to the training set and is used to test the candidate concept and determmine its performance.

# Issues in Decision Trees

1. **Avoiding Overfitting**:

   - allow the tree to grow until it overfits and then prune it
   - prevent the tree from growing too deep by stopping it before it perfectly classifies the training training data.

2. **Incorporating continuous valued attributes**

# Advantages of Decision Trees

1. Easy to use and understand
2. Can handle both categorical and numerical data
3. Resistant to outliers, hence require little data preprocessing
4. New features can be easily added
5. Can be used to build larger classifiers by using ensemble methods.

# Disadvantages of Decision Trees

1. prone to overfitting
2. require some kind of measurement as to how well they are doing.
3. need to be careful with parameter tuning
4. can create biased learned trees if some classes dominate
