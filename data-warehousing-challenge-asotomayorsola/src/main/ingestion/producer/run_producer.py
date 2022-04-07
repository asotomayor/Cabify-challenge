import mysql.connector
from mysql.connector import Error
import logging

from main.ingestion.producer.constants import TIMESTAMP_FORMAT, JDBC_DRIVER, USER, PASSWORD, APPEND
from main.ingestion.producer.create_pro_tables import CreateProTables
from main.ingestion.producer.create_raw_tables import CreateRawTables
from main.ingestion.producer.insert_pro_tables import get_pro_queries, read_stats_pro, insert_stats_pro
from main.ingestion.producer.raw_schemas import get_cars_schema, get_drivers_schema, \
    get_users_schema, get_trip_schema, get_raw_tables
from main.ingestion.run_ingestion import ConfIngestion, spark
from pyspark.sql import Window
from pyspark.sql import functions as f


class Producer(ConfIngestion):

    def __init__(self):
        self.confingestion = ConfIngestion
        self.create_raw_tables = CreateRawTables
        self.create_pro_tables = CreateProTables

    def raw_producer(self) -> (list, list):
        """
        Produce Raw and Pro dataframes for Mysql databases ingestion
        :return: List of raw and pro dataframes for each table into a tuple
        """
        try:
            # Open raw database Mysql connection
            connection_raw = mysql.connector.connect(host=self.host,
                                                     user=USER,
                                                     password=PASSWORD,
                                                     db=self.raw_database)
            cursor = connection_raw.cursor()

            logging.info("Creating Raw tables into cabify_raw_db")

            # Create raw tables into Mysql cabiby_raw_db
            cursor.execute(self.create_raw_tables.create_cars_raw())
            cursor.execute(self.create_raw_tables.create_drivers_raw())
            cursor.execute(self.create_raw_tables.create_users_raw())
            cursor.execute(self.create_raw_tables.create_trip_raw())

            cursor.close()
            connection_raw.close()

            logging.info("Generating Raw Dataframes from json input files")
            # Create raw dataframes from input json files
            cars_raw = (
                spark.read.schema(get_cars_schema()).json(self.cars_raw_path)
                .withColumn("product_ids", f.to_json("product_ids"))
                .withColumnRenamed("_id", "id")
                .withColumn("created_at", f.from_unixtime(f.unix_timestamp(f.col("created_at"), TIMESTAMP_FORMAT)))
                .withColumn("updated_at", f.from_unixtime(f.unix_timestamp(f.col("updated_at"), TIMESTAMP_FORMAT)))
            )
            drivers_raw = (
                spark.read.schema(get_drivers_schema()).json(self.drivers_raw_path)
                .withColumn("created_at", f.from_unixtime(f.unix_timestamp(f.col("created_at"), TIMESTAMP_FORMAT)))
                .withColumn("updated_at", f.from_unixtime(f.unix_timestamp(f.col("updated_at"), TIMESTAMP_FORMAT)))
            )
            users_raw = (
                spark.read.schema(get_users_schema()).json(self.users_raw_path)
                .withColumn("created_at", f.from_unixtime(f.unix_timestamp(f.col("created_at"), TIMESTAMP_FORMAT)))
                .withColumn("updated_at", f.from_unixtime(f.unix_timestamp(f.col("updated_at"), TIMESTAMP_FORMAT)))
            )
            trip_raw = (
                spark.read.schema(get_trip_schema()).json(self.trip_raw_path)
                .withColumn("stops", f.to_json("stops"))
                .withColumn("created_at", f.from_unixtime(f.unix_timestamp(f.col("created_at"), TIMESTAMP_FORMAT)))
                .withColumn("start_at", f.from_unixtime(f.unix_timestamp(f.col("start_at"), TIMESTAMP_FORMAT)))
                .withColumn("updated_at", f.from_unixtime(f.unix_timestamp(f.col("updated_at"), TIMESTAMP_FORMAT)))
            )

            # Create pro dataframes without duplicates and with the most recent values
            cars_pro = (
                cars_raw.filter((f.col("id").isNotNull()))
                .withColumn("max_updated_at", f.max("updated_at").over(Window.partitionBy("id")))
                .filter(f.col("updated_at") == f.col("max_updated_at"))
                .dropDuplicates(subset=["id"])
                .select(cars_raw.columns)
            )
            drivers_pro = (
                drivers_raw.filter((f.col("id").isNotNull()))
                .withColumn("max_updated_at", f.max("updated_at").over(Window.partitionBy("id")))
                .filter(f.col("updated_at") == f.col("max_updated_at"))
                .dropDuplicates(subset=["id"])
                .select(drivers_raw.columns)
            )
            users_pro = (
                users_raw.filter((f.col("id").isNotNull()))
                .withColumn("max_updated_at", f.max("updated_at").over(Window.partitionBy("id")))
                .filter(f.col("updated_at") == f.col("max_updated_at"))
                .dropDuplicates(subset=["id"])
                .select(users_raw.columns)
            )
            trip_pro = (
                trip_raw.filter((f.col("id").isNotNull()))
                .withColumn("max_updated_at", f.max("updated_at").over(Window.partitionBy("id")))
                .filter(f.col("updated_at") == f.col("max_updated_at"))
                .dropDuplicates(subset=["id"])
                .select(trip_raw.columns)
            )

            # Create raw and pro dataframes lists
            raw_list = [cars_raw, drivers_raw, users_raw, trip_raw]
            pro_list = [cars_pro, drivers_pro, users_pro, trip_pro]

            return raw_list, pro_list
        except Error as e:
            logging.warning("Error while creating Raw Dataframes", e)

    def raw_insertion(self, raw_list):
        """
        Insert Raw dataframes into into cabify_raw_db
        """
        logging.info("Inserting Raw Dataframes into cabify_raw_db")
        # Insert raw data into Mysql cabiby_raw_db tables
        try:
            for x, y in zip(raw_list, get_raw_tables()):
                x.write.format('jdbc').options(url=self.url + self.raw_database,
                                               driver=JDBC_DRIVER,
                                               dbtable=y,
                                               user=USER,
                                               password=PASSWORD).mode(APPEND).save()
        except Error as e:
            logging.info("Error while connecting to MySQL cabify_raw_db", e)

    def pro_insertion(self, pro_list):
        """
        Insert Pro dataframes into into cabify_pro_db
        """
        # Open pro database Mysql connection
        connection_pro = mysql.connector.connect(host=self.host,
                                                 user=USER,
                                                 password=PASSWORD,
                                                 db=self.pro_database)
        cursor = connection_pro.cursor()

        logging.info("Creating Pro tables into cabify_pro_db")

        # Create pro tables if not exits
        cursor.execute(self.create_pro_tables.create_cars_pro())
        cursor.execute(self.create_pro_tables.create_drivers_pro())
        cursor.execute(self.create_pro_tables.create_users_pro())
        cursor.execute(self.create_pro_tables.create_trip_pro())

        logging.info("Inserting Pro Dataframes into cabify_pro_db")

        # Insert or replace values into cabify_pro_db tables
        for x, y in zip(pro_list, get_pro_queries()):
            df = x.toPandas()
            for index, row in df.iterrows():
                cursor.execute(y, ([row[c] for c in df.columns]))
                connection_pro.commit()

        cursor.close()
        connection_pro.close()

    def pro_stats(self):
        # Open pro database Mysql connection
        try:
            connection_pro = mysql.connector.connect(host=self.host,
                                                     user=USER,
                                                     password=PASSWORD,
                                                     db=self.pro_database)
            if connection_pro.is_connected():
                db_info = connection_pro.get_server_info()

                logging.info("Connected to MySQL Server version ", db_info)

                cursor = connection_pro.cursor()

                # Create new stats table if not exists
                cursor.execute(self.create_pro_tables.create_stats_pro())

                # Execute aggregate queries over cabify_pro_db tables
                cursor.execute(read_stats_pro())
                data = cursor.fetchall()
                logging.info('Number of rows loaded', len(data))

                # Insert or update values into Mysql cabify_pro_db stats table
                try:
                    for row in data:
                        user, count_30_days, count_15_days, count_7_days, last_dop_off, \
                        frequent_driver, trips_frequent_driver, avg_price_trip = row
                        cursor.execute(insert_stats_pro(),
                                       (user, count_30_days, count_15_days, count_7_days, last_dop_off,
                                        frequent_driver, trips_frequent_driver, avg_price_trip))

                    connection_pro.commit()
                    logging.info("Record inserted successfully into stats_pro table")
                except mysql.connector.Error as error:
                    logging.warning("Failed to insert record to database: {}".format(error))
        except Error as e:
            logging.warning("Error while connecting to MySQL", e)
        finally:
            if connection_pro.is_connected():
                cursor.close()
                connection_pro.close()
                logging.info("MySQL connection is closed")