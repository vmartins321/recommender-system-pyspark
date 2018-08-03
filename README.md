# Overview

Collaborative filtering fills in the missing entries of a user-item association matrix by describing users and items with latent factors. The alternating least squares (ALS) algorithm learns these latent factors iteratively by holding one factor matrix e.g. users constant while solving for the other e.g. items, then alternating to the other matrix. This is a blocked implementation that reduces communication by only sending one copy of each user vector to each item block on each iteration, and only for the item blocks that need that a user's feature vector. Because we are using implicit preference data, the ratings act as confidence values for preferences 1 and 0.

# Instructions

* The DAG file is an Airflow job to manage the entire pipeline - creates training data, spins up the cluster, runs the job, and tears down the cluster.
* The ipynb file is a PySpark job that creates the recommendations and uploads the output to cloud storage.
