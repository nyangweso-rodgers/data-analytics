# K Means

## Table Of Contents

# Introduction To K Means
* __K-means__ is one of the most popular algorithms for __clustering__, a form of __unsupervised machine learning__ which aims to find groups of similar examples within a training dataset.

* The algorithm works by first initialising __random cluster centroids__. Then for each datapoint a distance measure commonly the __Euclidean distance__ or __Cosine similarity__ is used to assign it to the nearest centroid. Once all data points are assigned the centroid is moved to the mean of the assigned data points. These steps are repeated until the centroid assignment ceases to change.

# When To Use K Means
* K-means is only suited to unsupervised clustering.
* It is generally considered a good all-rounder algorithm for these types of problems.

# Advantages Of K Means
* It is a relatively simple algorithm to implement.
* It can be used on large datasets.
* The resulting clusters are easy to interpret.

# Disadvantages Of K Means
* K-means are sensitive to outliers.
* This algorithm does not find the optimal number of clusters. This has to be determined by other techniques prior to implementation
* The results of the clustering are not consistent. If K-means is run on a dataset multiple times it can produce different results each time.