import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import (
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# Calculate cosine similarity
def computeCosineSimilarity(spark, data):
    # Compute xx, xy and yy columnsmm
    pairScores = (
        data.withColumn("xx", func.col("rating1") * func.col("rating1"))
        .withColumn("yy", func.col("rating2") * func.col("rating2"))
        .withColumn("xy", func.col("rating1") * func.col("rating2"))
    )

    # Compute numerator, denominator and numPairs columns
    calculateSimilarity = pairScores.groupBy("movie1", "movie2").agg(
        func.sum(func.col("xy")).alias("numerator"),
        (
            func.sqrt(func.sum(func.col("xx"))) * func.sqrt(func.sum(func.col("yy")))
        ).alias("denominator"),
        func.count(func.col("xy")).alias("numPairs"),
    )

    # Calculate score and select only needed columns (movie1, movie2, score, numPairs, avg_rating1, avg_rating2)
    result = calculateSimilarity.withColumn(
        "score",
        func.when(
            func.col("denominator") != 0,
            func.col("numerator") / func.col("denominator"),
        ).otherwise(0),
    ).select("movie1", "movie2", "score", "numPairs")

    return result


# Get movie name by given movie id
def getMovieName(movieNames, movieID):
    result = (
        movieNames.filter(func.col("movieID") == movieID)
        .select("movieTitle")
        .collect()[0]
    )

    return result[0]


# Get avg rating by given movie id
def getMovieAvgRating(movies, movieID):
    avg_rating = (
        movies.filter(func.col("movieID") == movieID)
        .select(func.sum("rating") / func.count("rating"))
        .collect()[0]
    )

    return avg_rating[0]


if __name__ == "__main__":
    # Configure Spark
    spark = (
        SparkSession.builder.appName("MovieSimilarities")
        .master("local[*]")
        .getOrCreate()
    )

    # Create schemas
    movieNamesSchema = StructType(
        [
            StructField("movieID", IntegerType(), True),
            StructField("movieTitle", StringType(), True),
        ]
    )

    moviesSchema = StructType(
        [
            StructField("userID", IntegerType(), True),
            StructField("movieID", IntegerType(), True),
            StructField("rating", FloatType(), True),
        ]
    )

    # Create a broadcast dataset of movieID and movieTitle.
    # Apply ISO-885901 charset
    movieNames = (
        spark.read.option("sep", ",")
        .option("charset", "ISO-8859-1")
        .option("header", "true")
        .schema(movieNamesSchema)
        .csv("ml-latest-small/movies.csv")
    )

    # Load up movie data as dataset
    movies = (
        spark.read.option("sep", ",")
        .option("header", "true")
        .schema(moviesSchema)
        .csv("ml-latest-small/ratings.csv")
    )

    # Emit every movie rated together by the same user.
    # Self-join to find every combination.
    # Select movie pairs and rating pairs
    moviePairs = (
        movies.alias("ratings1")
        .join(
            movies.alias("ratings2"),
            (func.col("ratings1.userId") == func.col("ratings2.userId"))
            & (func.col("ratings1.movieId") < func.col("ratings2.movieId")),
        )
        .select(
            func.col("ratings1.movieId").alias("movie1"),
            func.col("ratings2.movieId").alias("movie2"),
            func.col("ratings1.rating").alias("rating1"),
            func.col("ratings2.rating").alias("rating2"),
        )
    )

    moviePairSimilarities = computeCosineSimilarity(spark, moviePairs).cache()

    # Run script only if the sys.argv is given
    if len(sys.argv) > 1:
        # SIMILARITY PARAMETERS - can be optimized
        scoreThreshold = 0.95
        coOccurrenceThreshold = 100.0
        ratingThreshold = 3.5
        numOfSimilarities = 10

        movieID = int(sys.argv[1])

        # Filter for movies with this sim that are "good" as defined by
        # quality thresholds above
        filteredResults = moviePairSimilarities.filter(
            (
                ((func.col("movie1") == movieID) | (func.col("movie2") == movieID))
                & (func.col("score") > scoreThreshold)
                & (func.col("numPairs") > coOccurrenceThreshold)
            )
        )

        # Sort by quality score and take top 100 movies (for the further filtering of top x (numOfSimilarities) movies)
        results = filteredResults.sort(func.col("score").desc()).take(100)

        # Prepare output title
        output = []
        output_txt = f"Top {numOfSimilarities} similar movies for {getMovieName(movieNames, movieID)} :"
        output.append(output_txt)

        print(output_txt)

        # Counter for output results wanted
        counter = 0

        # Loop through results
        for result in results:
            if counter >= numOfSimilarities:
                break
            # Display the similarity result that isn't the movie we're looking at
            similarMovieID = result.movie1
            if similarMovieID == movieID:
                similarMovieID = result.movie2

            # Get movie average rating
            movieAvgRating = getMovieAvgRating(movies, similarMovieID)
            if movieAvgRating < ratingThreshold:
                continue

            # Increase counter if movie meets the ratingThreshold requirement
            counter += 1

            # Find movie name by it's ID
            movie_name = getMovieName(movieNames, similarMovieID)

            # Format the name to fit the schema
            if len(movie_name) > 40:
                movie_name = movie_name[:37] + "..."

            formatted_name = "{:<40}".format(movie_name)
            formatted_score = "{:<6.4f}".format(result.score)
            formatted_strength = "{:<6}".format(result.numPairs)
            formatted_rating = "{:<6.2f}".format(movieAvgRating)

            # Output whole result according to the following
            output_result = "{} 	score: {} 	strength: {} 	rating: {}".format(
                formatted_name, formatted_score, formatted_strength, formatted_rating
            )
            print(output_result)
            output.append(output_result)

    """       # Write output to a text file
        with open("similar_movies.txt", "w") as f:
            f.write("\n".join(output)) """

    spark.stop()
