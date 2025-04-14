🎬 Project: Basic Movie Recommendation System
✅ Objective
Build a system that recommends the most popular and highest-rated movies for each genre using Spark SQL and PySpark.

🧱 Tools & Tech Stack
PySpark: Data processing

Spark SQL: Data analysis

HDFS: Store input CSV files

MySQL (optional): Store output or top recommendations

Jupyter Notebook: Your working environment
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
📂 Dataset
Use the classic MovieLens dataset – choose the small ml-latest-small.zip.

Files you'll use:

movies.csv → movieId, title, genres

ratings.csv → userId, movieId, rating, timestamp
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

Option : Use Google Colab + PySpark + Google Drive
You can run PySpark in Google Colab and load files from Google Drive.

Pros:
No setup required.

Can use Spark on Colab (limited but good enough).

Free GPU/TPU if needed.

Easy to share notebooks via GitHub or PDF.
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
✅ Step 1: Open Google Colab
Go to https://colab.research.google.com

Create a new notebook.

✅ Step 2: Install PySpark
Add this cell at the top:

python
Copy
Edit
!apt-get install openjdk-11-jdk-headless -qq > /dev/null
!wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz -O spark.tgz
!tar xf spark-3.5.0-bin-hadoop3.tgz
!pip install -q findspark

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.5.0-bin-hadoop3"

import findspark
findspark.init()
✅ Step 3: Start a SparkSession
python
Copy
Edit
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PySpark Colab") \
    .getOrCreate()

spark
✅ Step 4: Mount Google Drive (to access CSVs)
python
Copy
Edit
from google.colab import drive
drive.mount('/content/drive')
After this, you’ll see a link — authorize and paste the code.

✅ Step 5: Load Your CSVs from Drive
Let’s say you’ve uploaded movies.csv and ratings.csv into a folder:
/MyDrive/pyspark-movie-data/

python
Copy
Edit
movies_path = "/content/drive/MyDrive/pyspark-movie-data/movies.csv"
ratings_path = "/content/drive/MyDrive/pyspark-movie-data/ratings.csv"

moviesdf = spark.read.option("header", "true").option("inferSchema", "true").csv(movies_path)
ratingsdf = spark.read.option("header", "true").option("inferSchema", "true").csv(ratings_path)

moviesdf.show(5)
ratingsdf.show(5)
✅ Step 6: Continue with Your Movie Recommender Logic
You can now proceed exactly like you would in Jupyter:

python
Copy
Edit
from pyspark.sql.functions import split, explode

moviesdf = moviesdf.withColumn("genre", explode(split("genres", "\\|")))
moviesdf.createOrReplaceTempView("movies")
ratingsdf.createOrReplaceTempView("ratings")

spark.sql("""
    SELECT genre, title, ROUND(AVG(r.rating), 2) as avg_rating, COUNT(*) as total_ratings
    FROM movies m
    JOIN ratings r ON m.movieId = r.movieId
    GROUP BY genre, title
    HAVING total_ratings >= 10
    ORDER BY genre, avg_rating DESC
""").show(50, truncate=False)
🔚 Optional: Save as PDF or Share GitHub
You can download the notebook as PDF (File > Print > Save as PDF)

Or push it to GitHub for your resume
