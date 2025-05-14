from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    # Inicjalizacja Spark
    spark = SparkSession.builder \
        .appName("PyCharm Spark Lab9") \
        .master("local[*]") \
        .getOrCreate()

    try:
        # 1. Wczytaj dane
        df = spark.read \
            .option("header", "true") \
            .csv("input.csv")  # Plik w katalogu projektu

        # 2. Przetwórz dane
        df = df.withColumn("new_value", col("old_value") * 2)

        # 3. Zapisz wyniki
        df.write.mode("overwrite").csv("output")

        print("Zadanie wykonane pomyślnie!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()