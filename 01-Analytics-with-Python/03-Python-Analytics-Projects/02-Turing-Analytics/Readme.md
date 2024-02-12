# Turing Analytics

# Description

- Thare are three datasets for this challenge:
  1. `cardio_alco.csv`
  2. `cardio_base.csv`
     - Each row represents a person and a corresponding attributes like, age, height, weight, blood pressure, cholesterol level e.t.c
     - When asked about age, please calculate with age in years rounded down
  3. `covid_data.csv`
     - contains daily covid 19 cases for all countries in the world. Each row represents a calendar day. Row also contain some simple Information about countries, like population, percentage of the population over 65, GDP, and hospital beds per thousand inhabitants.

# Tasks

## Task 1

- Load `cardio_base.csv`
- This is a health dataset. Each row represents a person and corresponding attributes like age, height, weight, blood pressure, cholesterol level e.t.c.
- When asked about age, please calculate with age in years rounded down.
- **Question 1**:
  - How much heavier is the group age with the highest average weight than the age group with the lowest weight?

## Task 2

- **Question 2**:
  - How tall are the tallest 1% of people?

## Task 3

- **Question 3**:
  - What percentage of people are more than 2 standard deviations far from the average height?

## Task 4

- **Question 4**:
  - What percentage of population over 50 years old consume alcohol?
  - Also, use the `cardio_alco.csv` file and merge the datasets on `ID`. Ignore those persons who have no alcohol consumption Information!

## Task 5

- **Question 5**:
  - Which of the following statements is true with 95% confidence?
    1. Smokers weight less than non-smokers?
    2. Men have higher blood pressure than women?
    3. Smokers have higher cholesterol level than non-smokers?
    4. Smokers have higher blood pressure than non-smokers?

## Task 6

- Second dataset, Covid19 cases.
- This datasetcontains daily covid19 cases for all countries in the world. Each row represents a calendar day. Rows also conntain simple Information about the countries, like
  1. population
  2. percentage of population over 65
  3. GDP, and
  4. hospital beds per thousand inhabitants.
- Use the dataset to answer the following questions:
  - **Question 6**
    - When did the difference in the total number of confirmed cases between Italy and Germany become more than 10,000?

## Task 7:

- **Question 7**
  - Look at the cumulative number of confirmed cases in Italy between 2020-02-28 and 2020-03-20.
  - Fit an exponential function (y=Ae^(Bx)) to this set to express cumulative cases a function of days passed, by minimizing squared loss.
  - What is the difference between the exeponential curve and the total number of real cases on 2020-03-20

## Task 8

- **Question 8**
  - Which country has the 3rd highest death rate?
  - Death Rate = Total number of deaths per million inhabitants

## Task 9

- **Question 9**
  - What is the F1 score of the following statements?
  - Countries where more than 20% of the population is over 65 years old, have death rates over 50 per million inhabitants.
  - Ignore countries where any of the necessary Information is missing.

## Task 10

- **Question**:
  - Calculate Spearman Rank correlation for `cardio_base.csv`
  - Identify two features with the highest spearman rank correlation

### Task 11

- **Question 11**:
  - Calculate the probability that a country has a GDP of over $10,000 given that they have at least 5 hospital beds per 1000 inhabitants.
