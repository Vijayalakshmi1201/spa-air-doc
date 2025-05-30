from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, lower

# Initialize Spark
spark = SparkSession.builder \
    .appName("GlobalStreetFoodAnalysis") \
    .getOrCreate()

# Load dataset
df = spark.read.csv("/opt/airflow/csv_files/global_street_food.csv", header=True, inferSchema=True)

# Clean column names
df = df.withColumnRenamed("Typical Price (USD)", "Price_USD") \
       .withColumnRenamed("Dish Name", "Dish_Name") \
       .withColumnRenamed("Region/City", "Region_City")

# Count of vegetarian vs non-vegetarian dishes
veg_counts = df.groupBy("Vegetarian").count()

# Average price of dishes by country
avg_price_by_country = df.groupBy("Country") \
    .agg(avg("Price_USD").alias("Average_Price_USD"))

# Most popular cooking methods (top 5)
top_cooking_methods = df.groupBy("Cooking Method") \
    .count() \
    .orderBy(col("count").desc()) \
    .limit(5)

#  Dishes with the highest price (top 5)
top_expensive_dishes = df.orderBy(col("Price_USD").desc()).select("Dish_Name", "Country", "Price_USD").limit(5)

# Number of dishes per region
dishes_by_region = df.groupBy("Region_City").count()

# Show results 
veg_counts.show()
avg_price_by_country.show()
top_cooking_methods.show()
top_expensive_dishes.show()
dishes_by_region.show()

spark.stop()

 