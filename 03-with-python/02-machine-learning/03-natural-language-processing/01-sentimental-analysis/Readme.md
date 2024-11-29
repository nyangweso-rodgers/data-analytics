# Sentiment Analysis

## Table Of Contents

# What is Sentiment Analysis?

- **Sentiment Analysis** is a use case of [Natural Language Processing (NLP)]() and comes under the category of [text classification](). It involves classifying a text into various sentiments, such as positive or negative, Happy, Sad or Neutral, etc. Thus, the ultimate goal of sentiment analysis is to decipher the underlying mood, emotion, or sentiment of a text. This is also referred to as Opinion Mining.

# How Does Sentiment Analysis Work?

- Sentiment analysis in Python typically works by employing **NLP** techniques to analyze and understand the sentiment expressed in text. The process involves several steps:
  1. **Text Preprocessing**: The text cleaning process involves removing irrelevant information, such as special characters, punctuation, and stopwords, from the text data.
  2. **Tokenization**: The text is divided into individual words or tokens to facilitate analysis.
  3. **Feature Extraction**: The text extraction process involves extracting relevant features from the text, such as words, n-grams, or even parts of speech.
  4. **Sentiment Classification**: Machine learning algorithms or pre-trained models are used to classify the sentiment of each text instance. Researchers achieve this through supervised learning, where they train models on labeled data, or through pre-trained models that have learned sentiment patterns from large datasets.
  5. **Post-processing**: The sentiment analysis results may undergo additional processing, such as aggregating sentiment scores or applying threshold rules to classify sentiments as positive, negative, or neutral.
  6. **Evaluation**: Researchers assess the performance of the sentiment analysis model using evaluation metrics, such as:
     1. accuracy,
     2. precision,
     3. recall, or
     4. F1 score.

# Sentiment Analysis Using Python

# Ways to Perform Sentiment Analysis in Python (Python Libraries for Sentimental Analysis)

- Pytho offers several ways to perform sentimental analysis:
  1. Using Text Blob
  2. Using Vader
  3. Using Bag of Words Vectorization-based Models
  4. Using LSTM-based Models
  5. Using Transformer-based Models

# 1. Using Text Blob

- **Text Blob** is a Python library for NLP. Using **Text Blob** for sentiment analysis is quite simple. It takes text as an input and can return polarity and subjectivity as outputs.
- **TextBlob** is praised for its ease of use and adaptability while managing natural language processing (NLP) workloads.
  - **Polarity** determines the sentiment of the text. Its values lie in [-1,1] where -1 denotes a highly negative sentiment and 1 denotes a highly positive sentiment.
  - **Subjectivity** determines whether a text input is factual information or a personal opinion. Its value lies between [0,1] where a value closer to 0 denotes a piece of factual information and a value closer to 1 denotes a personal opinion.

## Steps

## Step 1. Installation

```sh
    pip install textblob
```

## Step2: Importing Text Blob

```py
    from textblob import TextBlob
```

## Step3: Code Implementation for Sentiment Analysis Using Text Blob

- Import the `TextBlob` object and pass the text to be analyzed with appropriate attributes as follows:

  ```py
    from textblob import TextBlob

    text_1 = "The movie was so awesome."
    text_2 = "The food here tastes terrible."

    #Determining the Polarity
    p_1 = TextBlob(text_1).sentiment.polarity
    p_2 = TextBlob(text_2).sentiment.polarity

    #Determining the Subjectivity
    s_1 = TextBlob(text_1).sentiment.subjectivity
    s_2 = TextBlob(text_2).sentiment.subjectivity

    print("Polarity of Text 1 is", p_1)
    print("Polarity of Text 2 is", p_2)
    print("Subjectivity of Text 1 is", s_1)
    print("Subjectivity of Text 2 is", s_2)
  ```

# Natural Language Toolkit (NLTK)

- An extensive and well-liked open-source package for Python **natural language processing** (**NLP**) is called the **Natural Language Toolkit** (**NLTK**).
- **NLTK**, which is well-known for its extensive collection of tools and resources, is capable of handling a number of **NLP** tasks, such as **tokenization**, **sentiment analysis**, **parsing**, and **semantic reasoning**.

# 2. Using Valence Aware Dictionary and Sentiment Reasoner (VADER)

- **VADER** (**Valence Aware Dictionary and Sentiment Reasoner**) is a sentiment analysis tool designed specifically for text on social media.
- **VADER** was created as a component of the NLTK package and is intended to handle colloquial language and expressions that are frequently encountered on social media sites like Facebook and Twitter. In place of machine learning, it employs a rule-based methodology in conjunction with a sentiment lexicon, in which words are pre-labeled with neutral, negative, or positive values.
- In order to assess text, **VADER** looks for sentiment-laden words and applies heuristic rules that take grammar and intensity into consideration. The entire sentiment is then reflected in a compound score that ranges from -1 to 1. Because VADER can scan enormous amounts of text quickly and accurately understand punctuation, emoticons, and slang to generate sentiment insights, it is particularly well-suited for social media surveillance.
