from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Tạo SparkSession
spark = SparkSession.builder.appName("SparkTextDataCleaning").getOrCreate()

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

# Loại bỏ dòng có null trong price hoặc quantity
df_cleaned = df.filter(col("price").isNotNull() & col("quantity").isNotNull())

#Lọc bỏ dữ liệu trùng (duplicate)
df_cleaned = df_cleaned.dropDuplicates()

# Chuẩn hóa tên sản phẩm (lowercase, xóa khoảng trắng dư)
df_cleaned = df_cleaned.withColumn("product", lower(trim(col("product"))))

# Hiển thị kết quả
print("DataFrame after cleaning:")
df_cleaned.show(5, truncate=False)
# Đóng SparkSession
spark.stop()