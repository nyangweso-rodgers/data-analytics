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

# Ways to Perform Sentiment Analysis in Python

- Pytho offers several ways to perform sentimental analysis:
  1. Using Text Blob
  2. Using Vader
  3. Using Bag of Words Vectorization-based Models
  4. Using LSTM-based Models
  5. Using Transformer-based Models

# 1. Using Text Blob

- Text Blob is a Python library for Natural Language Processing. Using Text Blob for sentiment analysis is quite simple. It takes text as an input and can return polarity and subjectivity as outputs.
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

# 2. Using VADER

- **VADER** (**Valence Aware Dictionary and Sentiment Reasoner**) is a rule-based sentiment analyzer that has been trained on social media text.
