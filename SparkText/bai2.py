from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Tạo SparkSession
spark = SparkSession.builder.appName("SparkTextBasicTransformations").getOrCreate()

# Định nghĩa schema với sale_date là StringType
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sale_date", StringType(), True)
])

# Đọc file CSV với schema đã định nghĩa
df = spark.read.csv("file:///C:/SparkText/sales_data.csv", header=True, schema=schema)

# Yêu cầu 1: Tính tổng doanh thu (price * quantity)
df = df.withColumn("revenue", col("price") * col("quantity"))

# Yêu cầu 2: Lọc các đơn hàng có doanh thu > 1000
df_filtered = df.filter(col("revenue") > 1000)  # Sửa từ filtered thành filter

# Yêu cầu 3: Đổi kiểu sale_date từ String sang DateType
from pyspark.sql.functions import to_date
df_filtered = df_filtered.withColumn("sale_date", to_date(col("sale_date"), "yyyy-MM-dd"))

# Hiển thị kết quả
print("DataFrame after calculating revenue, filtering, and converting sale_date to DateType:")
df_filtered.show(5, truncate=False)

# Kiểm tra schema sau khi chuyển đổi
print("Schema after transformation:")
df_filtered.printSchema()

# Đóng SparkSession
spark.stop()