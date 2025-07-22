# Pandas

## Table Of Contents

# Categorical Data Encoding Techniques

## 1. One-hot Encoding

- Each category is represented by a binary vector of 0s and 1s.
- Each category gets its own binary feature, and only one of them is "hot" (set to 1) at a time, indicating the presence of that category.
- Number of features = Number of unique categorical labels

## 2. Dummy Encoding

- Same as **one-hot encoding** but with one additional step.
- After **one-hot encoding**, we drop a feature randomly.
- This is done to avoid the dummy variable trap
- Number of features = Number of unique categorical labels - 1.

## 3. Effect Encoding

- Similar to **dummy encoding** but with one additional step.
- Alter the row with all zeros to -1.
- This ensures that the resulting binary features represent not only the presence or absence of specific categories but also the contrast between the reference category and the absence of any category.
- Number of features = Number of unique categorical labels - 1.

## 4. Label Encoding

- Assign each category a unique label.
- Label encoding introduces an inherent ordering between categories, which may not be the case.
- Number of features = 1.

## 5. Ordinal Encoding

- Similar to **label encoding** — assign a unique integer value to each category.
- The assigned values have an inherent order, meaning that one category is considered greater or smaller than another.
- Number of features = 1.

## 6. Count (Frequency) Encoding

- Encodes categorical features based on the frequency of each category.
- Thus, instead of replacing the categories with numerical values or binary representations, count encoding directly assigns each category with its corresponding count.
- Number of features = 1.

## 7. Binary Encoding

- Combination of **one-hot encoding** and **ordinal encoding**.
- It represents categories as binary code.
- Each category is first assigned an ordinal value, and then that value is converted to binary code.
- The binary code is then split into separate binary features.
- Useful when dealing with high-cardinality categorical features (or a high number of features) as it reduces the **dimensionality** compared to one-hot encoding.
- Number of features = log(n) (in base 2).

# Overview of Pandas Module

# Resources and Further Reading

1. [Daily Dose of Data Science - 7 Categorical Data Encoding Techniques](https://blog.dailydoseofds.com/p/7-categorical-data-encoding-techniques-1f5?ref=dailydev)
