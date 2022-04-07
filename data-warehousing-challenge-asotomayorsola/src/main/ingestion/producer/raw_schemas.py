from typing import List

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, DoubleType


def get_raw_tables() -> List[str]:
    return ['cars_raw', 'drivers_raw', 'users_raw', 'trip_raw']


def get_cars_schema() -> StructType:
    return StructType([
        StructField("_id", StringType(), True),
        StructField("reg_plate", StringType(), True),
        StructField("disabled", StringType(), True),
        StructField("icon", StringType(), True),
        StructField("licensed", StringType(), True),
        StructField("product_ids", ArrayType(StringType()), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])


def get_drivers_schema() -> StructType:
    return StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("state", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])


def get_users_schema() -> StructType:
    return StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("mobile_num", StringType(), True),
        StructField("email", StringType(), True),
        StructField("locale", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])


def get_trip_schema() -> StructType:
    return StructType([
        StructField("id", StringType(), True),
        StructField("car", StringType(), True),
        StructField("driver", StringType(), True),
        StructField("user", StringType(), True),
        StructField("price", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("stops", ArrayType(StructType([
            StructField("country", StringType(), True),
            StructField("loc", ArrayType(DoubleType(), True))]))),
        StructField("created_at", TimestampType(), True),
        StructField("start_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])