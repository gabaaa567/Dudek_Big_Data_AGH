from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def read_data(spark, path):
    """Wczytuje dane z pliku CSV z separatorem średnika."""
    return spark.read \
        .option("header", "true") \
        .option("delimiter", ";") \
        .csv(path)


def transform_data(df):
    """Przefiltruj wiersze, gdzie 'value' jest niepuste."""
    return df.filter(col("value").isNotNull())


def write_data(df, path):
    """Zapisz dane jako CSV."""
    df.write.mode("overwrite").csv(path, header=True)


if __name__ == "__main__":
    import os
    print("JAVA_HOME:", os.environ.get("JAVA_HOME"))

    spark = SparkSession.builder \
        .appName("Spark Lab 9") \
        .getOrCreate()

    input_path = "data/input.csv"
    output_path = "output/result"

    df = read_data(spark, input_path)

    print("== Podgląd danych po wczytaniu ==")
    df.show()

    df_transformed = transform_data(df)
    df_transformed.show()

    write_data(df_transformed, output_path)
    spark.stop()


