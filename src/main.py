from pyspark.sql import SparkSession
from src.config import INPUT_PATH_A, INPUT_PATH_B, OUTPUT_PATH, TOP_X
from utils import (rm_duplicates, map_to_item_count, reduce_count, restructure_count,
                   map_location_dict, attach_location_name, group_and_get_top_items)


def main():
    spark = SparkSession.builder.appName("TopItemsDetection").getOrCreate()

    # Read parquet files in DF
    df_a = spark.read.parquet(INPUT_PATH_A)
    df_b = spark.read.parquet(INPUT_PATH_B)

    # Convert to RDD
    rdd_a = df_a.rdd
    rdd_b = df_b.rdd

    # Rm duplicates
    unique_rdd = rm_duplicates(rdd_a)

    #// Count items per loc
    # Map each record into ((location_oid, item_name), ##)
    # E.g. ((100, "Car"), 1)
    mapped_rdd = unique_rdd.map(map_to_item_count)

    # Aggregate by key (location, item), sum up counts
    # E.g. ((100, "Car"), 2), ((100, "Car"), 3)) -> ((100, "Car"), 2)
    item_counts_rdd = mapped_rdd.reduceByKey(reduce_count)

    # Take aggregated item counts
    # E.g. ((100, "Car"), 2)
    input_rdd = item_counts_rdd

    # Restructure record
    # E.g. (location_oid, (item_name, count))
    location_items_rdd = input_rdd.map(restructure_count)

    # Group by loc, get top X items
    grouped_rdd = group_and_get_top_items(location_items_rdd, TOP_X)

    #// Broadcast loc names
    input_locations_rdd = rdd_b

    # Convert each row to (location_oid, location_name)
    # E.g. (100, "Car")
    pairs_rdd = input_locations_rdd.map(map_location_dict)

    # Convert into python dict {}
    # E.g. {100:"Car", 101:"Lorry"}
    loc_dict = dict(pairs_rdd.collect())

    # Broadcast dict{}
    broadcast_loc = spark.sparkContext.broadcast(loc_dict)

    # Attach loc names
    def attach_names(record):
        return attach_location_name(record, broadcast_loc)

    final_rdd = grouped_rdd.map(attach_names)

    # Convert RDD->DF and save
    final_df = final_rdd.toDF(["geographical_location_oid", "geographical_location", "item_rank", "item_name"])
    final_df.write.mode("overwrite").parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
