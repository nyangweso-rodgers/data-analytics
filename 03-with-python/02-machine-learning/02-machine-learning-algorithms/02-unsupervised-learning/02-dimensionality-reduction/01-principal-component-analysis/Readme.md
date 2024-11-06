# Principal Component Analysis (PCA)

## Table of Contents

# Principal Component Analysis (PCA)

- **PCA** is a **dimensionality reduction** technique. It’s often used to reduce the number of features in a dataset while retaining most of the variance (information).

# Example 1: Principal Component Analysis (PCA)

- **Problem Statement**: Reduce the dimensionality of the Iris dataset to 2 components.
- Code:

  ```python
      from sklearn.decomposition import PCA
      from sklearn.datasets import load_iris

      # Load the Iris dataset
      iris = load_iris()
      X = iris.data

      # Initialize PCA with 2 components
      pca = PCA(n_components=2)
      X_reduced = pca.fit_transform(X)

      # Print the reduced feature set
      print("Reduced feature set:\n", X_reduced[:5])
  ```

- Output:
