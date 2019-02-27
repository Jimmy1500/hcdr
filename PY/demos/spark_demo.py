#!/usr/bin/python3

import pyspark
import pyspark.sql
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import expr, col, column

sc=SparkContext('local')
sc.setLogLevel("ERROR")
spark=SparkSession(sc)

test_data = spark.read.option("inferSchema", "true").option("header", "true").csv("../../data/application_test.csv")
test_data.createOrReplaceTempView("APP_TEST_DATA")

# [Option 1] create new data frame by SQL
print("# [Option 1] create new data frame by SQL")
df=spark.sql(""" SELECT NAME_EDUCATION_TYPE, NAME_FAMILY_STATUS FROM APP_TEST_DATA """ )
df.explain()
df.show(2)

spark.sql("""                                                                                                                       \
SELECT AMT_INCOME_TOTAL, AMT_CREDIT, FLAG_OWN_REALTY, NAME_HOUSING_TYPE, NAME_INCOME_TYPE, NAME_EDUCATION_TYPE, NAME_FAMILY_STATUS  \
FROM APP_TEST_DATA                                                                                                                  \
WHERE NAME_EDUCATION_TYPE == "Higher education"                                                                                     \
AND   NAME_FAMILY_STATUS == "Married"                                                                                               \
AND   FLAG_OWN_REALTY == "Y"                                                                                                        \
AND   NAME_HOUSING_TYPE like "House%"                                                                                               \
""").show(12)

spark.sql(""" SELECT COUNT(DISTINCT(NAME_EDUCATION_TYPE)) FROM APP_TEST_DATA """).show()

# [Option 2] create new data frame by spark API
print("# [Option 2] create new data frame by spark API")
df=test_data.select("NAME_EDUCATION_TYPE", "NAME_FAMILY_STATUS")
df.explain()
df.show(2)

test_data.selectExpr("AMT_INCOME_TOTAL", "AMT_CREDIT", "FLAG_OWN_REALTY", "NAME_HOUSING_TYPE", "NAME_INCOME_TYPE", "NAME_EDUCATION_TYPE", "NAME_FAMILY_STATUS") \
         .where(col("NAME_EDUCATION_TYPE") == "Higher education")                                                                                               \
         .where(col("NAME_FAMILY_STATUS") == "Married")                                                                                                         \
         .where(col("FLAG_OWN_REALTY") == "Y")                                                                                                                  \
         .where(col("NAME_HOUSING_TYPE").contains("House")).show(12)

test_data.selectExpr("count(distinct(NAME_EDUCATION_TYPE))").show()


# Concatenating and Appending Rows(Union)
# train_data = spark.read.option("inferSchema", "true").option("header", "true").csv("../../data/application_train.csv")
# train_data.createOrReplaceTempView("APP_TRAIN_DATA")

# data_schema = train_data.schema
# new_rows = [ Row("New Country", "Other Country", 5L), Row("New Country 1", "Other Country 1", 1L) ]
# parallelized_rows = spark.sparkContext.parallelize(new_rows)
# new_df = spark.createDataFrame(parallelized_rows, schema)
# test_data.union(new_df).where(col("NAME_EDUCATION_TYPE") == "Higher education").where(col("NAME_FAMILY_STATUS") == "Married").show()

# app_data = test_data.union(train_data).where(col("NAME_EDUCATION_TYPE") == "Higher education").where(col("NAME_FAMILY_STATUS") == "Married")
# app_data.show()
