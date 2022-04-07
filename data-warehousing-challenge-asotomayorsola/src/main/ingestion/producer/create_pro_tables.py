from dataclasses import dataclass
from typing import List


@dataclass
class CreateProTables:
    """Create Raw Tables"""

    def __init__(self):
        create_cars_pro: staticmethod
        create_drivers_pro: staticmethod
        create_users_pro: staticmethod
        create_trip_pro: staticmethod
        create_stats_pro: staticmethod

    @staticmethod
    def get_pro_tables() -> List[str]:
        return ['cars_pro', 'drivers_pro', 'users_pro', 'trip_pro']

    @staticmethod
    def create_cars_pro() -> str:
        return '''CREATE TABLE IF NOT EXISTS cars_pro (
                                id VARCHAR(40) NOT NULL,
                                reg_plate VARCHAR(40),
                                disabled VARCHAR(5),
                                icon VARCHAR(40),
                                licensed VARCHAR(40),
                                product_ids JSON,
                                created_at DATETIME,
                                updated_at DATETIME,
                                PRIMARY KEY (id)
                                )'''

    @staticmethod
    def create_drivers_pro() -> str:
        return '''CREATE TABLE IF NOT EXISTS drivers_pro (
                                id VARCHAR(40) PRIMARY KEY,
                                name VARCHAR(40),
                                state VARCHAR(40),
                                created_at DATETIME,
                                updated_at DATETIME
                                )'''

    @staticmethod
    def create_users_pro() -> str:
        return '''CREATE TABLE IF NOT EXISTS users_pro (
                                id VARCHAR(50) PRIMARY KEY,
                                name VARCHAR(50),
                                mobile_num VARCHAR(50),
                                email VARCHAR(50),
                                locale VARCHAR(50),
                                created_at DATETIME,
                                updated_at DATETIME
                                )'''

    @staticmethod
    def create_trip_pro() -> str:
        return '''CREATE TABLE IF NOT EXISTS trip_pro (
                               id VARCHAR(50) PRIMARY KEY,
                               car VARCHAR(50),
                               driver VARCHAR(50),
                               user VARCHAR(50),
                               price DECIMAL(10,2),
                               reason VARCHAR(50),
                               stops JSON,
                               created_at DATETIME,
                               start_at DATETIME,
                               updated_at DATETIME
                               )'''

    @staticmethod
    def create_stats_pro() -> str:
        return '''CREATE TABLE IF NOT EXISTS stats_pro (
                               user VARCHAR(50) PRIMARY KEY,
                               count_30_days INT,
                               count_15_days INT,
                               count_7_days INT,
                               last_dop_off DATETIME,
                               frequent_driver VARCHAR(50),
                               trips_frequent_driver INT,
                               avg_price_trip DECIMAL(10,2)
                               )'''