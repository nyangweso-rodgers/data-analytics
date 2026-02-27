# XGBoost (eXtreme Gradient Boosting)

## Table of Contents

# XGBoost

---

## What is XGBoost?

- **XGBoost** is an **ensemble learning method** based on gradient boosting decision trees. It builds models sequentially, where each new tree corrects the errors of the previous ones. Think of it as assembling a team of weak learners (decision trees) that collectively become a strong predictor.
- **Key Innovation**: **XGBoost** adds **regularization** to prevent overfitting and uses a more principled approach to tree building, making it faster and more accurate than traditional **gradient boosting**.

---

## Common Use Cases

1. Financial Services
   - Credit scoring and loan default prediction
   - Fraud detection
   - Risk assessment
   - Customer churn prediction

2. Healthcare
   - Disease diagnosis and prediction
   - Patient readmission risk
   - Treatment outcome prediction

3. E-commerce & Marketing
   - Customer lifetime value prediction
   - Recommendation systems (as a ranking component)
   - Click-through rate prediction
   - Conversion optimization

4. Insurance
   - Claims prediction
   - Premium pricing

5. Manufacturing
   - Predictive maintenance
   - Quality control

---

## Performance Metrics: Precision, Recall, and F-Score

### Precision

- Formula: `TP / (TP + FP)`
- "Of all the positive predictions, how many were correct?"
- Use when false positives are costly (e.g., spam detection - you don't want legitimate emails marked as spam)

### Recall (Sensitivity)

- Formula: `TP / (TP + FN)`
- "Of all actual positives, how many did we catch?"
- Use when false negatives are costly (e.g., cancer detection - you don't want to miss actual cases)

### F1-Score

- Formula: `2 × (Precision × Recall) / (Precision + Recall)`
- Harmonic mean of **precision** and **recall**
- Useful when you need a balance between precision and recall
- **XGBoost** can optimize directly for F1-score or use it as an evaluation metric

### AUC-ROC: Area under the ROC curve

- Great for imbalanced datasets

### Log Loss

- Measures probability calibration

### RMSE/MAE

- For regression tasks

---

## Key Features & Advantages

1. Regularization
   - L1 (Lasso) and L2 (Ridge) regularization built-in
   - Prevents overfitting better than basic **gradient boosting**

2. Handling Missing Values
   - Automatically learns the best direction to handle missing data
   - No need for manual imputation

3. Speed & Efficiency
   - Parallel processing
   - Cache optimization
   - Out-of-core computing for large datasets

4. Flexibility
   - Custom objective functions
   - Custom evaluation metrics
   - Supports classification, regression, and ranking problems

5. Feature Importance
   - Provides built-in feature importance scores
   - Helps with feature selection and model interpretability

---

- **XGBoost** is an algorithm based on **gradient-boosted decision trees**. It is similar to **Random Forest** in that it builds an ensemble of decision trees but rather than training the models in parallel, **XGBoost** trains the models sequentially. Each decision tree learns from the errors produced by the previous model. This technique of training models sequentially is known as **boosting**.

- The gradient in **XGBoost** refers to a specific type of boosting where **weak learners** are used. **Weak learners** are very simple models that only just perform better than random chance. The algorithm starts with an initial weak learner. Each subsequent model targets the errors produced by the previous decision tree. This continues until no further improvement can be made and results in a final strong learner model.

- **When To Use XGBoost?**
  1. It can be used to solve both **classification** and **regression-based** problems.
  2. **XGBoost** is generally considered one of the best and most flexible algorithms for supervised learning on structured data and is therefore suited to a wide range of datasets and problem types.

- **Advantages Of XGBoost**
  1. XGboost is highly flexible in that it works equally well on small and large datasets.
  2. It is computationally efficient and therefore faster to train models compared to other complex algorithms.

- **Disadvantages Of XGBoost**
  1. It does not work as well on very sparse or unstructured data.
  2. It is considered a black box model and is less interpretable than some other algorithms.
  3. **XGBoost** can be sensitive to outliers due to the mechanism of models learning from the errors of their predecessors.

# Resources and Further Reading
