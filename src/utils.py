from pyspark.sql import DataFrame


def get_product_category_pairs(
    products: DataFrame, categories: DataFrame, product_category_map: DataFrame
) -> DataFrame:
    """Get all product-category pairs and products with no category."""
    return (
        products.join(
            product_category_map, products.id == product_category_map.product_id, "left"
        )
        .join(categories, categories.id == product_category_map.category_id, "left")
        .select(
            products.name.alias("product_name"), categories.name.alias("category_name")
        )
        .orderBy(products.id, categories.id)
    )
