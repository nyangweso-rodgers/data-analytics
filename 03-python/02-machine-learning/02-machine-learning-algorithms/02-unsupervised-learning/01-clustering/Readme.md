# Clustering Algorithms

## Table Of Contents

# Introduction to Clustering Algorithms

- Clustering models aim to group data into distinct “clusters” or groups.
- In **clustering**, we do not have a target to predict. We look at the data and then try to club similar observations and form different groups. Hence it is an **unsupervised learning** problem.

# Properties of Clusters

- All the data points in a cluster should be similar to each other
- The data points from different clusters should be as different as possible

# Applications of Clustering in Real-World Scenarios

- Customer Segmentation
- Document Clustering
- Image Segmentation
- Recommendation Engines

# Understanding the Different Evaluation Metrics for Clustering

1. **Inertia**: calculates the sum of distances of all the points within a cluster from the centroid of that cluster. _NOTE_: the lesser the inertia value, the better our clusters are.

2. **Dunn Index**: Along with the distance between the centroid and points, the _Dunn index_ also takes into account the distance between two clusters. This distance between the centroids of two different clusters is known as **inter-cluster** distance.

   - **Dunn Index** = min(Inter Cluster distance) / max(Intra Cluster distance)

   - **REMARK**: _Dunn index is the ratio of the minimum of inter-cluster distances and maximum of intracluster distances. We want to maximize the Dunn index. The more the value of the Dunn index, the better will be the clusters._
