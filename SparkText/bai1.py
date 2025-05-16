from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, count, when

# Tạo SparkSession
spark = SparkSession.builder.appName("SparkTextExploration").getOrCreate()

# Định nghĩa schema
schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("sale_date", DateType(), True)
])

# Đọc file CSV với đường dẫn tuyệt đối
df = spark.read.csv("file:///C:/SparkText/sales_data.csv", header=True, schema=schema)

# Yêu cầu 1: In schema
print("Schema of DataFrame:")
df.printSchema()

# Yêu cầu 2: Hiển thị 5 dòng đầu tiên
print("First 5 rows of DataFrame:")
df.show(5, truncate=False)

# Yêu cầu 3: Kiểm tra dữ liệu null
print("Number of null values in each column:")

df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# Yêu cầu 4: Kiểm tra kiểu dữ liệu
print("Data types of columns:")
for column, dtype in df.dtypes:
    print(f"Column {column}: {dtype}")

# Đóng SparkSession
spark.stop()