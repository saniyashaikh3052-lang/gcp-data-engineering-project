from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    when,
    trim,
    lower
)

from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType
)


# ─── Create Spark Session ────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("TMDB Silver Transformation")
    .master("local[*]")
    .getOrCreate()
)


# ─── Read Bronze JSON File ───────────────────────────────────────────────────
file_path = r"data/raw/2026-05-18/trending_raw_20260518_090539.json"

print(f"\nReading file: {file_path}")


df = (
    spark.read
    .option("multiline", "true")
    .json(file_path)
)


# ─── Print Raw Bronze Schema ─────────────────────────────────────────────────
print("\nRaw Bronze Schema:\n")

df.printSchema()


# ─── Show Raw Sample Records ─────────────────────────────────────────────────
print("\nRaw Bronze Sample Data:\n")

df.show(5, truncate=False)


# ─── Select Important Analytics Columns ──────────────────────────────────────
silver_df = df.select(
    col("id"),
    col("media_type"),
    col("title"),
    col("name"),
    col("original_language"),
    col("popularity"),
    col("vote_average"),
    col("vote_count"),
    col("release_date"),
    col("first_air_date"),
    col("genre_ids"),
    col("_snapshot_date"),
    col("_source_page")
)


# ─── Create Unified Content Title ────────────────────────────────────────────
silver_df = silver_df.withColumn(
    "content_title",
    when(
        col("title").isNotNull(),
        col("title")
    ).otherwise(col("name"))
)


# ─── Remove Duplicate Records ────────────────────────────────────────────────
silver_df = silver_df.dropDuplicates(["id"])


# ─── Fill NULL Values ────────────────────────────────────────────────────────
silver_df = silver_df.fillna({
    "original_language": "unknown",
    "vote_average": 0.0,
    "vote_count": 0,
    "popularity": 0.0
})


# ─── Trim + Standardize String Columns ───────────────────────────────────────
silver_df = (
    silver_df
    .withColumn(
        "content_title",
        trim(col("content_title"))
    )
    .withColumn(
        "media_type",
        lower(trim(col("media_type")))
    )
    .withColumn(
        "original_language",
        lower(trim(col("original_language")))
    )
)


# ─── Conditional Categorization Using when() ─────────────────────────────────
silver_df = silver_df.withColumn(
    "content_category",
    when(
        col("vote_average") >= 8,
        "Highly Rated"
    ).when(
        col("vote_average") >= 6,
        "Popular"
    ).otherwise("Average")
)


# ─── Expected Schema Definition ──────────────────────────────────────────────
EXPECTED_SCHEMA = StructType([

    StructField("id", LongType(), nullable=True),

    StructField("media_type", StringType(), nullable=True),

    StructField("content_title", StringType(), nullable=True),

    StructField("original_language", StringType(), nullable=True),

    StructField("popularity", DoubleType(), nullable=True),

    StructField("vote_average", DoubleType(), nullable=True),

    StructField("content_category", StringType(), nullable=True)

])


# ─── Schema Validation Function ──────────────────────────────────────────────
def validate_schema(df, expected_schema):

    expected_fields = {
        field.name: field.dataType
        for field in expected_schema.fields
    }

    actual_fields = {
        field.name: field.dataType
        for field in df.schema.fields
    }

    missing_columns = (
        set(expected_fields)
        - set(actual_fields)
    )

    if missing_columns:

        raise ValueError(
            f"Missing columns: {missing_columns}"
        )

    for column_name, expected_type in expected_fields.items():

        actual_type = actual_fields.get(column_name)

        if actual_type != expected_type:

            raise TypeError(
                f"""
                Column '{column_name}'
                expected {expected_type}
                got {actual_type}
                """
            )

    print("\n✅ Schema validation passed")


# ─── Run Schema Validation ───────────────────────────────────────────────────
validate_schema(silver_df, EXPECTED_SCHEMA)


# ─── Print Final Silver Schema ───────────────────────────────────────────────
print("\nFinal Silver Schema:\n")

silver_df.printSchema()


# ─── Show Final Silver Sample Data ───────────────────────────────────────────
print("\nFinal Silver Sample Data:\n")

silver_df.select(
    "id",
    "media_type",
    "content_title",
    "vote_average",
    "content_category"
).show(10, truncate=False)


# ─── Save Silver Layer As Parquet ────────────────────────────────────────────
silver_output_path = (
    "data/silver/tmdb/trending_all/day_json/"
)

(
    silver_df.write
    .mode("overwrite")
    .json(silver_output_path)
)

print(f"\n✅ Silver JSON saved at: {silver_output_path}")

# ─── Stop Spark Session ──────────────────────────────────────────────────────
spark.stop()