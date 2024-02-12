# Clustering Algorithms

## Table Of Contents
- [K-Means Clustering]

# Introduction to Clustering Algorithms
* In __clustering__, we do not have a target to predict. We look at the data and then try to club similar observations and form different groups. Hence it is an __unsupervised learning__ problem.

# Properties of Clusters
* All the data points in a cluster should be similar to each other
* The data points from different clusters should be as different as possible

# Applications of Clustering in Real-World Scenarios
* Customer Segmentation
* Document Clustering
* Image Segmentation
* Recommendation Engines

# Understanding the Different Evaluation Metrics for Clustering
1. __Inertia__: calculates the sum of distances of all the points within a cluster from the centroid of that cluster. _NOTE_: the lesser the inertia value, the better our clusters are.

2. __Dunn Index__: Along with the distance between the centroid and points, the _Dunn index_ also takes into account the distance between two clusters. This distance between the centroids of two different clusters is known as __inter-cluster__ distance. 

    * __Dunn Index__ = min(Inter Cluster distance) / max(Intra Cluster distance)

    * __REMARK__: _Dunn index is the ratio of the minimum of inter-cluster distances and maximum of intracluster distances. We want to maximize the Dunn index. The more the value of the Dunn index, the better will be the clusters._