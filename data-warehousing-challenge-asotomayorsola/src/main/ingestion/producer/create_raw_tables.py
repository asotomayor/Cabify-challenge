from dataclasses import dataclass


@dataclass
class CreateRawTables:

    """Create Raw Tables"""

    def __init__(self):
        create_cars_raw: staticmethod
        create_drivers_raw: staticmethod
        create_users_raw: staticmethod
        create_trip_raw: staticmethod

    @staticmethod
    def create_cars_raw() -> str:
        return '''CREATE TABLE IF NOT EXISTS cars_raw (
                                id VARCHAR(40),
                                reg_plate VARCHAR(40),
                                disabled VARCHAR(5),
                                icon VARCHAR(40),
                                licensed VARCHAR(5),
                                product_ids VARCHAR(50),
                                created_at VARCHAR(50),
                                updated_at VARCHAR(50)
                                )'''

    @staticmethod
    def create_drivers_raw() -> str:
        return '''CREATE TABLE IF NOT EXISTS drivers_raw (
                                id VARCHAR(40),
                                name VARCHAR(40),
                                state VARCHAR(40),
                                created_at VARCHAR(50),
                                updated_at VARCHAR(50)
                                )'''

    @staticmethod
    def create_users_raw() -> str:
        return '''CREATE TABLE IF NOT EXISTS users_raw (
                                id VARCHAR(50),
                                name VARCHAR(50),
                                mobile_num VARCHAR(50),
                                email VARCHAR(50),
                                locale VARCHAR(50),
                                created_at VARCHAR(50),
                                updated_at VARCHAR(50)
                                )'''

    @staticmethod
    def create_trip_raw() -> str:
        return '''CREATE TABLE IF NOT EXISTS trip_raw (
                                id VARCHAR(50),
                                car VARCHAR(50),
                                driver VARCHAR(50),
                                user VARCHAR(50),
                                price VARCHAR(50),
                                reason VARCHAR(50),
                                stops JSON,
                                created_at VARCHAR(50),
                                start_at VARCHAR(50),
                                updated_at VARCHAR(50)
                                )'''