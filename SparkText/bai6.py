from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, sum as sum_
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Tạo SparkSession
spark = SparkSession.builder.appName("SparkTextUnionMonthlyRevenue").getOrCreate()

# Định nghĩa schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sale_date", DateType(), True)
])

# Đọc file cụ thể
df = spark.read.option("header", "true").schema(schema).csv("file:///C:/SparkText/sales_data.csv")

# Thêm cột revenue (price * quantity)
df = df.withColumn("revenue", col("price") * col("quantity"))

# Tính doanh thu theo từng tháng
df_monthly = df.groupBy(
    year("sale_date").alias("year"),
    month("sale_date").alias("month")
).agg(sum_("revenue").alias("monthly_revenue"))

# Hiển thị kết quả
print("Monthly revenue:")
df_monthly.orderBy("year", "month").show(truncate=False)

# Đóng SparkSession
spark.stop()