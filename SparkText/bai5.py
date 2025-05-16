from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, row_number, min
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# Tạo SparkSession
spark = SparkSession.builder.appName("SparkTextWindowFunctions").getOrCreate()

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

# Thêm cột revenue
df = df.withColumn("revenue", col("price") * col("quantity"))

# Tính doanh thu tích lũy theo ngày
window_spec_cumulative = Window.orderBy("sale_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df = df.withColumn("cumulative_revenue", sum_("revenue").over(window_spec_cumulative))

# Tính thứ hạng sản phẩm theo doanh thu trong từng category
window_spec_rank = Window.partitionBy("category").orderBy(col("revenue").desc())

# Tìm ngày đầu tiên bán trên mỗi sản phẩm
window_spec_first_sale = Window.partitionBy("product").orderBy("sale_date")
df_first_sale = df.withColumn("first_sale_date", min("sale_date").over(window_spec_first_sale))

# Lấy duy nhất ngày đầu tiên cho mỗi sản phẩm
df_first_sale_unique = df_first_sale.dropDuplicates(["product"]).select("product", "first_sale_date")

# Hiển thị kết quả
print("DataFrame with cumulative revenue and revenue rank:")
df.show(5, truncate=False)

print("First sale date for each product:")
df_first_sale_unique.show(truncate=False)

# Đóng SparkSession
spark.stop()