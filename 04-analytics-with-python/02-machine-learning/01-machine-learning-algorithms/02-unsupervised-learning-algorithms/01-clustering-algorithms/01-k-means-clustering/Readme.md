# K-Means Clustering

## Table Of Contents

# Introduction To K Means

- **K-means** is one of the most popular algorithms for **clustering**, a form of **unsupervised machine learning** which aims to find groups of similar examples within a training dataset.

* **K-means** is a _centroid-based algorithm_, or a _distance-based algorithm_, where we calculate the distances to assign a point to a **cluster**. In **K-Means**, each cluster is associated with a **centroid**. The main objective of the **K-Means** algorithm is to _minimize the sum of distances between the points and their respective cluster centroid_.

- The algorithm works by first initialising **random cluster centroids**. Then for each datapoint a distance measure commonly the **Euclidean distance** or **Cosine similarity** is used to assign it to the nearest **centroid**. Once all data points are assigned the centroid is moved to the mean of the assigned data points. These steps are repeated until the centroid assignment ceases to change.

# When To Use K Means

- K-means is only suited to unsupervised clustering.
- It is generally considered a good all-rounder algorithm for these types of problems.

# Advantages Of K Means

- It is a relatively simple algorithm to implement.
- It can be used on large datasets.
- The resulting clusters are easy to interpret.

# Disadvantages Of K Means

- K-means are sensitive to outliers.
- This algorithm does not find the optimal number of clusters. This has to be determined by other techniques prior to implementation
- The results of the clustering are not consistent. If K-means is run on a dataset multiple times it can produce different results each time.

# Steps to Creating Clusters with K-means

1. **Step 1:** Choose the number of clusters k

2. **Step 2:** _Select k random points from the data as centroids_. Next, we randomly select the centroid for each cluster. Let’s say we want to have 2 clusters, so k is equal to 2 here. We then randomly select the centroid.

3. **Step 3:** Assign all the points to the closest cluster centroid

4. **Step 4:** _Recompute the centroids of newly formed clusters_. Now, once we have assigned all of the points to either cluster, the next step is to compute the centroids of newly formed clusters:

5. **Step 5:** Repeat steps 3 and 4

- **Remarks**:
  - The maximum possible number of **clusters** will be equal to the number of observations in the dataset
  - How to Choose the Right Number of Clusters in K-Means Clustering?
    - One thing we can do is plot a graph, also known as an **elbow curve**, where the **x-axis will represent the number of clusters** and the **y-axis will be an evaluation metric**. The cluster value where the decrease in **inertia** value becomes constant can be chosen as the right cluster value for our data.
    - You must also look at the **computation cost** while deciding the number of clusters. If we increase the number of clusters, the computation cost will also increase. So, if you do not have high computational resources, my advice is to choose a lesser number of clusters.

# Steps

## Step 1. Import Python Modules

```py
    from sklearn.datasets import make_blobs
    from sklearn.cluster import KMeans

    X, y_true = make_blobs(n_samples=250, centers=3,
                        cluster_std=0.60, random_state=0)

    kmeans = KMeans(n_clusters=3)
    kmeans.fit(X)
    y_kmeans = kmeans.predict(X)
```

# Resources And Further Reading
