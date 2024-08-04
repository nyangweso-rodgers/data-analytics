# Implementing K-means Clustering Algorithm

# Introduction to K-Means Clustering

- **K-means clustering** is an unsupervised algorithm. It operates without any predefined labels or training examples. This algorithm is used to group similar data points in a dataset. The goal is to divide the data into clusters. Each cluster contains similar data points.
- The algorithm works by:
  1. **Initialization**: Choose the number of clusters (k). Initialize k points randomly as centroids
  2. **Assignment**: Assign each data point to the nearest centroid and form the clusters.
  3. **Update Centroids**: Calculate the mean of all data points assigned to each centroid. Move the centroid to this mean position.
- Repeat steps 2 and 3 until convergence.

# Resources and Further Reading

1. [https://machinelearningmastery.com/using-machine-learning-in-customer-segmentation/?ref=dailydev](https://machinelearningmastery.com/using-machine-learning-in-customer-segmentation/?ref=dailydev)
