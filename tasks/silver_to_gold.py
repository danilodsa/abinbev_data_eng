from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import logging

# Spark session
try:
    spark = SparkSession.builder \
        .appName("breweriesProcessor") \
        .getOrCreate()

except Exception as e:
    logging.error(f"Failed to create Spark session: {str(e)}")
    raise    

# Load data from silver
silver_df = spark.read.parquet("./data/silver/breweries/breweries.parquet")
deduplicated_silver = silver_df.dropDuplicates(["id"])

# agg breweries by type and location
agg_breweries_type_city = (
    deduplicated_silver
        .groupBy(
            col("brewery_type"),
            col("city"),
            col("state_province")
        )
        .agg(
            count("*")
            .alias("brewery_count")
        )
)

agg_breweries_type_state = (
    deduplicated_silver
        .groupBy(
            col("brewery_type"),
            col("state_province")
        )
        .agg(
            count("*")
            .alias("brewery_count")
        )
)

# Save data
agg_breweries_type_city.write \
    .mode("overwrite") \
    .partitionBy("city") \
    .save("./data/gold/breweries/breweries_per_city.parquet")   

agg_breweries_type_state.write \
    .mode("overwrite") \
    .partitionBy("state_province") \
    .save("./data/gold/breweries/breweries_per_state.parquet") 