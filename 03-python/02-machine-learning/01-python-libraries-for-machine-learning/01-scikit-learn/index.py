import numpy as np
from sklearn import datasets
# Load data
iris= datasets.load_iris()
# Print shape of data to confirm data is loaded
#print(iris.data.shape)

x = np.array([[0.1, 1.0, 22.8],[0.5, 5.0, 41.2],[1.2, 12.0, 2.8],[0.8, 8.0, 14.0]])

print(x)