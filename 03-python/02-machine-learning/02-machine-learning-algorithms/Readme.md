# Machine Learning Algorithms

## Table Of Contents

# Definition of Machine Learning

- Machine learning is a subfield of artificial intelligence that focuses on developing algorithms and statistical models that enable computers to perform tasks that would normally require human intelligence, such as recognizing patterns, making predictions, and taking decisions. The ultimate goal of machine learning is to create models that can learn from and make predictions about data, without being explicitly programmed to perform these tasks.

# Applications of Machine Learning

1. Image Recognition
2. Product Recommendation
3. Automatic Vehicles
4. Spam Filtering
5. Online Fraud Detection
6. Medical Diagnostics
7. Automatic Language Transalation

# Types Of Machine Learning Algorithms

## 1. Supervised Learning Algorithms

- In **Supervised Learning**, we have access to data and labels(which correspond to the final results or conclusions). In this, the machine learning algorithm tries to use the data to predict its corresponding label.
- Categories of Supervised Learning algorithms
  1. **Classification**: A **classification** problem requires the algorithm to classify an entity based on certain attributes. Classification is binary if there are only two classes which generally consist of yes or no type of classification and it is multi-class if there exist more than two types of classes. A good example can be an algorithm that classifies whether a given patient with certain attributes regarding their cardiovascular health has heart disease or not.
  2. **Regression**: A regression problem involves predicting a quantity(more specifically a number). A label can go up or down and can take any value based on the inputs. An example of a regression problem can be trying to predict the selling price of a car based on certain attributes like the interior design, the specification of alloys used, the capacity of the engine, the installed music system, and much more.

## 2. Unsupervised Learning Algorithm

- In **Unsupervised Learning**, we have access to the data only. There are no labels, to begin with. The task here is to find the commonalities in the data and then cluster the data into certain groups. This process is also known as **clustering**. There are no labels, the machine learning algorithm finds patterns in the data itself and helps to make clusters of information that can be referred to as a group.
- Examples:
  1. A good example can be the real-life implementation of the music/show recommendations you get. Based on the clicks you make, you can be clustered into a certain group and then get recommendations.

## 3. Reinforcement Learning

- **Reinforcement learning** involves having a computer perform some actions within a defined space and rewarding it for doing well and punishing it for doing poorly. We can't physically punish or reward the computer, but what we can do is maintain a score that gets an increment when the performance is good and as expected and gets a decrement when the performance is poor. The goal will be to attain a predefined good score. A good example of transfer learning can be to teach a computer how to play chess. Reinforcement learning is not as evolved as other types of machine learning problems and would probably find more real-world applications in the near future.

# Metrics to Evaluate Machine Learning Algorithms

## 1. Confusion Matrix

- A **confusion matrix** is an **N X N matrix**, where **N** is the number of classes being predicted. It is usually used to **describe the performance of a classification model based on the true values known for a data set**. i.e., it is a summary of the prediction result made by the **classification model**.
- Terms associated with confusion matrix are:
  1. **True Positive** (**TP**): True positive means their actual class value is 1 and the predicted value is also 1.
     - Example: The case where the woman is actually pregnant and the model also classifies that she is pregnant.
  2. **False Positive** (**FP**): False positive means their actual class value is 0 and the predicted value is 1.
     - Example: The case where the woman is not pregnant and the model classifies that she is pregnant.
  3. **True Negative** (**TN**): True negative means their actual class value is 0 and the predicted value is also 0.
     - Example: The case where the woman is actually not pregnant and the model also classifies that she is not pregnant.
  4. **False Negative** (**FN**): False-negative means their actual class value is 1 and the predicted value is 0.
     - Examples: The case where the woman is actually pregnant and the model also classifies that she is not pregnant.

## 2. Accuracy

- **Accuracy** is the most common metric that you can see everywhere when you're evaluating your model. It is simply defined as the number of correct predictions made by your model. i.e., **accuracy is the ratio of all the correct predictions to the total number of predictions**. (**number of times you predicted something correctly divided by how many times you actually predicted it**)
- Formula:
  - **Accuracy = (True Positive + True Negative) / (Total Predictions)**
- Remark:
  - Accuracy is not a suitable metric to use in situations where the data is skewed

## 3. Prrecision

- **Precision** is defined as the **total number of correctly classified positive examples by the total number of predicted positive examples**
- In some cases, your classification model might classify based on the most frequent classes. Which in turn will bring a low accuracy because your model didn’t learn anything and just classified based on the top class.
- Therefore, we need class-specific performance metrics to analyze. Precision is one of them.
- Formula:
  - **Precision = (True Positive) / (True Positive + False Positive)**
- Remark:
  - Precision depicts how much the model is right when it says it is right.

## 4. Recall

- A **recall** (also known as **sensitivity**) refers to the percentage of total relevant results correctly classified by the **classification model**. It is the the **number of positive samples returned by the custom-trained model**.
- Formula:
  - **Recall = (True Positive) / (True Positive + False Negative)**
- Remark:
  - Recall tells us how good our model is at identifying relevant samples.
- **Note** (**Relation Between Recall and Precision**):
  1. High recall, low precision: This means, most of the positive examples are correctly recognized but there are a lot of false positives.
  2. Low recall, high precision: This means that we missed a lot of positive examples but those we predicted as positive are indeed positive.

## 5. F1 Score

- Based on the use case of the **classification model**, the priority is given either to **precision** or **recall**, but in some classification models, we need both of these metrics to be combined as a single one.
- **F1-Score is a metric that combines both precision and recall** and has an equal and relative contribution of both precision and recall.
- It is the harmonic mean of precision and recall.
- **F1 Score** = 2 x (Precision x Recall) / (Precision + Recall)
- **Remark**:
  - Remember that if your use case needs either recall or precision, one higher than the other then F1-score may not be the good metric for it.

## 6. Mean Absolute Error (MAE)

- **MAE** is the **average of all errors that are calculated based on values predicted by your model**.
- It is intended to measure average model bias in a set of predictions, without considering their direction.

## 7. R Squared Score (Coefficient Of Determineation)

- **R squared** is a measure of how close the data are to the fitted regression line.
- It defines the degree to which the variance in the dependent variable (or target) can be explained by the independent variable (features). For example, if the **R-squared** value for our predictive model is 0.8. This means that 80% of the variation in the dependent variable is explained by the independent variables.
- NOTE:
  - The higher the r-squared value is, the better is the model.

## 8. Root Mean Squared Error (RMSE)

- **RMSE** is one of the most popular metrics used today for evaluating regression-based models. This is an important evaluation metric since it’s essential to find the average squared error between the predicted values.
- **RMSE** measures the average magnitude of the error. It’s the square root of the average of squared differences between prediction and actual observation.
- Remarks:
  - **RMSE** is highly affected by outlier values. Hence, make sure you’ve removed outliers from your data set prior to using this metric.
  - As compared to **mean absolute error**, RMSE gives higher weightage and punishes large errors
  - Correlation between **MAE** and **RMSE**
    - Both of these metrics express the average error of the machine learning models. These two metrics can range from 0 to infinity and both of these metrics are negatively oriented scores, which means that a lower score defines better results.

## 9. Receiver Operating Characteristic (ROC) Curve

- The ROC curve is a graph showing the performance of a classification model at all its cut-off thresholds. i.e., the ROC Curve is the one that tells how much your model is capable of differentiating among the different classes.
- ROC is a probability curve. It is a representation of the performance of your model in a graphical manner.
- This curve represents:
  1. True positive rate (recall/sensitivity)
  2. False-positive rate (1- specificity): FPR = FP/ FP+TN
- The curve separates the space into two areas, one for good and the other for poor performance levels.

# Resources and Further Reading

1. [Machine Learning Algorithms Cheat Sheet](https://towardsdatascience.com/machine-learning-algorithms-cheat-sheet-2f01d1d3aa37)
