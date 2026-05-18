from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    avg,
    count,
    desc
)


# ─── Create Spark Session ────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("TMDB Gold Layer")
    .master("local[*]")
    .getOrCreate()
)


# ─── Read Bronze JSON Data ───────────────────────────────────────────────────
file_path = r"data/raw/2026-05-18/trending_raw_20260518_090539.json"

df = (
    spark.read
    .option("multiline", "true")
    .json(file_path)
)


# ─── Create Unified Content Title ────────────────────────────────────────────
gold_df = df.withColumn(
    "content_title",
    col("title")
)

gold_df = gold_df.fillna({
    "content_title": "Unknown"
})


# ─── Top Trending Content ────────────────────────────────────────────────────
top_trending_df = (
    gold_df
    .select(
        "content_title",
        "media_type",
        "popularity",
        "vote_average"
    )
    .orderBy(
        desc("popularity")
    )
)

print("\nTop Trending Content:\n")

top_trending_df.show(10, truncate=False)


# ─── Average Rating By Media Type ────────────────────────────────────────────
avg_rating_df = (
    gold_df
    .groupBy("media_type")
    .agg(
        avg("vote_average").alias("avg_rating")
    )
)

print("\nAverage Rating By Media Type:\n")

avg_rating_df.show()


# ─── Content Count By Language ───────────────────────────────────────────────
language_count_df = (
    gold_df
    .groupBy("original_language")
    .agg(
        count("*").alias("content_count")
    )
    .orderBy(
        desc("content_count")
    )
)

print("\nContent Count By Language:\n")

language_count_df.show(10)


# ─── Most Popular Content ────────────────────────────────────────────────────
most_popular_df = (
    gold_df
    .select(
        "content_title",
        "popularity",
        "vote_average"
    )
    .orderBy(
        desc("vote_average"),
        desc("popularity")
    )
)

print("\nHighest Rated Content:\n")

most_popular_df.show(10, truncate=False)


# ─── Stop Spark Session ──────────────────────────────────────────────────────
spark.stop()