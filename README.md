# Hadoop-Clustering
Create an inverted index using Hadoop's map-reduce utilities and use K-means algorithm to cluster data.

The first map-reduce phase receives different text files as input (as well as number of files, number of centers and number of iterations for the k-means algorithm) and outputs an inverted index for the words present in the documents.
Each following map-reduce phase represents an iteration of the k-means algorithm until the desired number of iterations has been reached and the final cluster centers have been determined.
A final map-reduce phase outputs each cluster ID and the words associated with it according to the performed k-means clustering algorithm.
