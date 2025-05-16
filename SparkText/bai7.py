from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Tạo SparkSession
spark = SparkSession.builder.appName("SparkTextJoinBroadcast").getOrCreate()

# Định nghĩa schema cho products.csv
products_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), True),
    StructField("supplier_id", IntegerType(), True)
])

# Định nghĩa schema cho suppliers.csv
suppliers_schema = StructType([
    StructField("supplier_id", IntegerType(), False),
    StructField("supplier_name", StringType(), True)
])

# Đọc hai file CSV với schema đã định nghĩa
products_df = spark.read.csv("file:///C:/SparkText/products.csv", header=True, schema=products_schema)
suppliers_df = spark.read.csv("file:///C:/SparkText/suppliers.csv", header=True, schema=suppliers_schema)

# Yêu cầu: Join 2 bảng để lấy tên supplier cho từng sản phẩm
# Sử dụng broadcast join vì suppliers nhỏ
joined_df = products_df.join(broadcast(suppliers_df), "supplier_id", "inner")

# Hiển thị kết quả
print("Joined DataFrame with supplier names:")
joined_df.show(truncate=False)

# Đóng SparkSession
spark.stop()