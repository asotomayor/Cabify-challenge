import configparser

from main.ingestion.producer.constants import MYSQL_JAVA_CONNECTOR
from pyspark.sql import SparkSession


class ConfIngestion:
    configParser = configparser.RawConfigParser()
    configPath = r'src/main/resources/conf/cabify.conf'
    configParser.read(configPath)
    cars_raw_path = configParser.get('Paths', 'cars_raw_path')
    drivers_raw_path = configParser.get('Paths', 'drivers_raw_path')
    users_raw_path = configParser.get('Paths', 'users_raw_path')
    trip_raw_path = configParser.get('Paths', 'trip_raw_path')
    url = configParser.get('DatabaseInfo', 'url')
    host = configParser.get('DatabaseInfo', 'HOST')
    raw_database = configParser.get('DatabaseInfo', 'RAW_DATABASE')
    pro_database = configParser.get('DatabaseInfo', 'PRO_DATABASE')


# Create Spark Session and set UTC time zone as data time zone
spark = (
    SparkSession.builder.config("spark.jars", MYSQL_JAVA_CONNECTOR)
    .master("local[1]")
    .appName('Cabify_Challenge')
    .config('spark.sql.session.timeZone', 'UTC')
    .getOrCreate()
)

# Set spark login INFO level
spark.sparkContext.setLogLevel("INFO")


def main():
    from main.ingestion.producer.run_producer import Producer
    (raw_list, pro_list) = Producer().raw_producer()
    Producer().raw_insertion(raw_list)
    Producer().pro_insertion(pro_list)


if __name__ == '__main__':
    main()
