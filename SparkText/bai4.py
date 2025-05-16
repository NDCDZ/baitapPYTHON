from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, StringType

# Tạo SparkSession
spark = SparkSession.builder.appName("SparkTextUDFAdvanced").getOrCreate()

# Định nghĩa schema
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

# Viết UDF gán nhãn price
def label_price(price):
    if price < 100:
        return "low"
    elif 100 <= price <= 500:
        return "medium"
    else:
        return "high"

# Đăng ký UDF
label_price_udf = udf(label_price, StringType())

# Tạo cột mới price_label dùng UDF
df = df.withColumn("price_label", label_price_udf(col("price")))

# Tính tỉ lệ price_label theo từng category
# Đăng ký bảng tạm
df.createOrReplaceTempView("sales")

# Sử dụng SQL để tính tỉ lệ
ratio_query = """
    SELECT 
        category, 
        price_label, 
        COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY category) AS label_ratio
    FROM sales
    GROUP BY category, price_label
    ORDER BY category, price_label
"""
ratio_df = spark.sql(ratio_query)

# Hiển thị kết quả
print("DataFrame with price_label:")
df.show(5, truncate=False)

print("Price label ratio by category:")
ratio_df.show(truncate=False)

# Đóng SparkSession
spark.stop()