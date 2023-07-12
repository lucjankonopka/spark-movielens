# Apache Spark - Similar Movies Finder

Technology used: *AWS (EMR on EC2, S3), Apache Spark, pyspark, python*

## Abstract

The main goal of the project was to analyze 27 million movie ratings for 58,000 movies provided by 280,000 users.
Due to the large volume of data, I utilized a cluster of three m5.xlarge instances to process the data.
I used PuTTY to log into the cluster. The outcome of the project was a list of the 10 most similar
movies to "Star Wars: Episode IV - A New Hope" from 1977, along with their ratings and similarity scores/strengths. 

## Process

1. Creating EMR cluster with three m5.xlarge ($0.224/hr per instance) and set up PuTTy terminal
2. Creating s3 bucket with input data and python script
3. Preparing movie-similarities-27m.py script using pyspark.sql
4. Running cluster and submitting the work
5. Terminating cluster (the job was done in about 15 minutes)
6. Reviewing the data


## Source code

[*movie-similarities-27m.py*](https://github.com/lucjankonopka/spark-movielens/blob/main/movie-similarities-27m.py)

## Algorithm

The score was calculated using cosine similarity as a measure of similarity between the two given movies, 
whose ratings are used here as similarity vectors:
![alt text](https://www.machinelearningplus.com/wp-content/uploads/2018/10/Cosine-Similarity-Formula-1.png)

The strength of similarity means the number of occurrences of a given pair. 
The rating is the average rating of a given movie. 

Any movie in the available database could be used for the similarity comparison. 
The selection of the movie took place after giving its ID as a command-line argument.

## Output

The outcome was a
[*text file*](https://github.com/lucjankonopka/spark-movielens/blob/main/similar_movies.txt)
that presented 10 movies with the highest similarity to "Star Wars: Episode IV - A New Hope" according to the algorithm.
