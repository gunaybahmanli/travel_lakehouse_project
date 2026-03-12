from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim, when, lower, upper, regexp_replace, current_timestamp, input_file_name, initcap, substring_index

def normalize_empty_strings(df: DataFrame) -> DataFrame:
    #Converting empty values to null

    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df =df.withColumn(
                field.name,
                when(trim(col(field.name)) == "", None).otherwise(trim(col(field.name)))
            )

    return df

def add_technical_columns(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("source_file", input_file_name())
        .withColumn("bronze_loaded_at", current_timestamp())
    )

def transform_countries(df: DataFrame) -> DataFrame:
    df = normalize_empty_strings(df)

    return(
        df
        .withColumnRenamed("country_short_description", "country_description")
        .withColumn("country_name", trim(col("country_name")))
        .withColumn("currency_name", initcap(col("currency_name")))
        .withColumn("country_code", upper(col("country_code")))
        .withColumn("currency_code", upper(col("currency_code")))
        .withColumn("language_main", lower(col("language_main")))
        .withColumn("is_active", when(lower(col("is_active")).isin("true", "1", "yes", "y"), True)
                                .when(lower(col("is_active")).isin("false", "0", "no", "n"), False)
                                .otherwise(None)
                    )
        .withColumn("visa_requirement_category", when(lower(col("visa_requirement_category")).isin("true", "1", "yes", "y"), "Yes")
                                .when(lower(col("visa_requirement_category")).isin("false", "0", "no", "n"), "No")
                                .otherwise("Unknown")
                    )
        .transform(add_technical_columns)
    )



string_to_remove = "(Gem) "

def transform_cities(df: DataFrame) -> DataFrame:
    df = normalize_empty_strings(df)

    return(
        df
        .withColumn("city_name", trim(col("city_name")))
        .withColumn("city_name_local", trim(col("city_name_local")))
        .withColumn("country_id", when(col("country_id") == 44, 4)
                                .otherwise(col("country_id"))
                    )
        .withColumn("is_capital", when(lower(col("is_capital")).isin("true", "1", "yes", "y"), "Yes")
                                .when(lower(col("is_capital")).isin("false", "0", "no", "n"), "No")
                                .otherwise(None)
                    )
        .withColumn("coastal_flag", when(lower(col("coastal_flag")).isin("true", "1", "yes", "y"), "Yes")
                                .when(lower(col("coastal_flag")).isin("false", "0", "no", "n"), "No")
                                .otherwise(None)
                    )
        .withColumn("mountain_flag", when(lower(col("mountain_flag")).isin("true", "1", "yes", "y"), "Yes")
                                .when(lower(col("mountain_flag")).isin("false", "0", "no", "n"), "No")
                                .otherwise(None)
                    )
        .withColumn("city_description", regexp_replace(col("city_description"), string_to_remove, ""))
        .transform(add_technical_columns)
    )


def transform_attractions(df: DataFrame) -> DataFrame:
    df =normalize_empty_strings(df)

    return(
        df
        .withColumn("attraction_name", trim(col("attraction_name")))
        .withColumn("attraction_type", lower(trim(col("attraction_type"))))
        .withColumn("city_name", trim(col("city_name")))
        .withColumn("family", when(lower(col("family")).isin("true", "1", "yes", "y"), True)
                              .when(lower(col("family")).isin("false", "0", "no", "n"), False)
                              .otherwise(None))
        .withColumn("currency", upper(col("currency")))
        .withColumn("city_id", when((col("country_id") == 2) | (col("city_id").isNull()), 4)
                              .when((col("country_id") == 8) | (col("city_id").isNull()), 22)  
                              .otherwise(col("city_id"))
                    )
        .transform(add_technical_columns)
    )



def transform_local_food(df: DataFrame) -> DataFrame:
    df = normalize_empty_strings(df)

    return(
        df
        .withColumnRenamed("avg_price", "avg_price_local")
        .withColumnRenamed("popularity", "popularity_score_raw")
        .withColumn("country_id", substring_index("country_id", " ", 1))
        .withColumn("food_name", trim(col("food_name")))
        .withColumn("food_category", lower(trim(col("food_category"))))
        .withColumn("is_vegetarian", when(lower(col("is_vegetarian")).isin("true", "1", "yes", "y"), True)
                                     .when(lower(col("is_vegetarian")).isin("false", "0", "no", "n"), False)
                                     .otherwise(None))
        .withColumn("is_spicy", when(lower(col("is_spicy")).isin("true", "1", "yes", "y"), True)
                                .when(lower(col("is_spicy")).isin("false", "0", "no", "n"), False)
                                .otherwise(None))
        .withColumn("currency", upper(col("currency")))
        .transform(add_technical_columns)
    )


def transform_restaurants(df: DataFrame) -> DataFrame:
    df = normalize_empty_strings(df)

    return(
        df
        .withColumnRenamed("rest_id", "restaurant_id")
        .withColumnRenamed("c_id", "country_id")
        .withColumnRenamed("rating", "rating_raw")
        .withColumnRenamed("reviews", "review_count_raw")
        .withColumnRenamed("ranking", "ranking_position_raw")
        .withColumnRenamed("hours", "opening_hours_raw")
        .withColumn("restaurant_name", trim(col("restaurant_name")))
        .withColumn("cuisine_type", lower(trim(col("cuisine_type"))))
        .withColumn("price_tier", lower(trim(col("price_tier"))))
        .transform(add_technical_columns)
    )

def transform_country_costs(df: DataFrame) -> DataFrame:
    df = normalize_empty_strings(df)

    return(
        df
        .withColumn("budget_level", lower(trim(col("budget_level"))))
        .withColumn("currency_code", upper(col("currency_code")))
        .transform(add_technical_columns)
    )


def write_bronze(df: DataFrame, output_path: str) -> None:
    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("static-raw-to-bronze")
        .getOrCreate()
    )

    datasets = [
        {
            "name": "countries",
            "input_path": "s3a://raw/static/countries/",
            "output_path": "s3a://bronze/static/countries/",
            "transform_func": transform_countries,
        },

        {
            "name": "cities",
            "input_path": "s3a://raw/static/cities/",
            "output_path": "s3a://bronze/static/cities/",
            "transform_func": transform_cities,
        },

        {
            "name": "attractions",
            "input_path": "s3a://raw/static/attractions/",
            "output_path": "s3a://bronze/static/attractions/",
            "transform_func": transform_attractions,
        },

        {
            "name": "local_food",
            "input_path": "s3a://raw/static/local_foods/",
            "output_path": "s3a://bronze/static/local_foods/",
            "transform_func": transform_local_food,
        },

        {
            "name": "restaurants",
            "input_path": "s3a://raw/static/restaurants/",
            "output_path": "s3a://bronze/static/restaurants/",
            "transform_func": transform_restaurants,
        },

        {
            "name": "country_costs",
            "input_path": "s3a://raw/static/country_costs/",
            "output_path": "s3a://bronze/static/country_costs/",
            "transform_func": transform_country_costs,
        }
    ]

    for dataset in datasets:
        print(f"\nProcessing dataset: {dataset['name']}")
        print(f"Reading: {dataset['input_path']}")

        df_raw = spark.read.parquet(dataset["input_path"])
        df_bronze = dataset["transform_func"](df_raw)

        print(f"Schema for {dataset['name']}:")
        df_bronze.printSchema()

        print(f"Sample rows for {dataset['name']}:")
        df_bronze.show(5, truncate = False)

        write_bronze(df_bronze, dataset["output_path"])
        print(f"Written to: {dataset['output_path']}")

    print(f"\nAll static datasets were successfully transformed to bronze.")
    spark.stop()

if __name__ == "__main__":
    main()


