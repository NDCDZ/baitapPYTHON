from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Tạo SparkSession
spark = SparkSession.builder.appName("SparkTextSQLQueries").getOrCreate()

# Định nghĩa schema với sale_date là DateType
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sale_date", DateType(), True)
])

# Đọc file CSV với schema đã định nghĩa
df = spark.read.csv("file:///C:/SparkText/sales_data.csv", header=True, schema=schema)

# Thêm cột revenue (price * quantity)
df = df.withColumn("revenue", col("price") * col("quantity"))

# Đăng ký bảng tạm sales
df.createOrReplaceTempView("sales")

# Tổng doanh thu theo category
total_revenue_query = """
    SELECT category, SUM(revenue) AS total_revenue
    FROM sales
    GROUP BY category
"""
total_revenue_df = spark.sql(total_revenue_query)

# Tìm sản phẩm có doanh thu cao nhất theo từng category
max_revenue_query = """
    SELECT category, product, revenue
    FROM (
        SELECT category, product, revenue,
               ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC) AS rank
        FROM sales
    )
    WHERE rank = 1
"""
max_revenue_df = spark.sql(max_revenue_query)

# Hiển thị kết quả
print("Total revenue by category:")
total_revenue_df.show(truncate=False)

print("Product with highest revenue per category:")
max_revenue_df.show(truncate=False)

# Đóng SparkSession
spark.stop()