# NLP

## Table Of Contents

- **Natural Language Processing** (NLP) is not a ML method per se, but rather a widely used technique to prepare text for ML. At the moment, the most popular package for processing text is **NLTK**(**Natural Language ToolKit**), created by researchers at Stanford.

* The simplest way to map text into a numerical representation is to compute the frequency of each word within each text document. Think of a matrix of integers where each row represents a text document and each column represents a word. This matrix representation of word frequencies is commonly called **Term Frequency Matrix** (**TFM**). From there, we create another popular matrix representation of a text document by dividing each entry on the matrix by a weight of how important each word within the entire corpus of documents. We call this method **Term Frequency Inverse Document Frequency** (**TFIDF**) and it typically works better for ML tasks.

- In every **NLP** project, text needs to be vectorized in order to be processed by machine learning algorithms. **Vectorization** methods are:
  1. one-hot encoding,
  2. counter encoding,
  3. frequency encoding,
  4. and word vector or word embeddings.
