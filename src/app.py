from pyspark.sql import DataFrame, SparkSession
from utils import get_product_category_pairs

spark = SparkSession.builder.getOrCreate()

products: DataFrame = spark.createDataFrame(
    [
        (1, "Laptop"),
        (2, "Smartphone"),
        (3, "Tablet"),
        (4, "Catchers in the Rye"),
        (5, "Harry Potter"),
        (6, "Refrigerator"),
        (7, "Washing Machine"),
        (8, "Vacuum Cleaner"),
        (9, "Drill"),
        (10, "Wet Wipes"),
    ],
    ["id", "name"],
)
products.show()
categories: DataFrame = spark.createDataFrame(
    [(1, "electronics"), (2, "books"), (3, "appliances"), (4, "tools"), (5, "home")],
    ["id", "name"],
)
product_category_map: DataFrame = spark.createDataFrame(
    [
        (1, 1),
        (2, 1),
        (3, 1),
        (4, 2),
        (5, 2),
        (6, 3),
        (6, 5),
        (7, 3),
        (7, 5),
        (8, 3),
        (8, 5),
        (9, 3),
    ],
    ["product_id", "category_id"],
)

pairs: DataFrame = get_product_category_pairs(
    products, categories, product_category_map
)
pairs.show()
