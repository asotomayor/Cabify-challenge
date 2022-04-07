def get_pro_queries():
    return [insert_cars_pro(), insert_drivers_pro(), insert_users_pro(), insert_trip_pro()]


def insert_cars_pro() -> str:
    return """REPLACE INTO cars_pro (id, reg_plate, disabled, icon, licensed, product_ids, created_at, updated_at) 
                                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""


def insert_drivers_pro() -> str:
    return """REPLACE INTO drivers_pro (id, name, state, created_at, updated_at) 
                                     VALUES (%s, %s, %s, %s, %s)"""


def insert_users_pro() -> str:
    return """REPLACE INTO users_pro (id, name, mobile_num, email, locale, created_at, updated_at) 
                                     VALUES (%s, %s, %s, %s, %s, %s, %s)"""


def insert_trip_pro() -> str:
    return """REPLACE INTO trip_pro (id, car, driver, user, price, reason, stops, created_at, start_at, updated_at) 
                                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""


def read_stats_pro() -> str:
    return """WITH tmp_7 AS (
                    SELECT user, COUNT(*) AS count_7_days
                    FROM trip_pro
                    WHERE reason = 'drop_off'
                    AND updated_at > (select MAX(updated_at) FROM trip_pro) - INTERVAL 7 day
                    GROUP BY user 
                    ORDER BY 1 DESC
            ), tmp_15 AS (
                    SELECT user, COUNT(*) as count_15_days
                    FROM trip_pro
                    WHERE reason = 'drop_off'
                    AND updated_at > (select MAX(updated_at) FROM trip_pro) - INTERVAL 15 day
                    GROUP BY user 
                    ORDER BY 1 DESC
            ), tmp_30 AS (
                    SELECT user, COUNT(*) as count_30_days
                    FROM trip_pro
                    WHERE reason = 'drop_off'
                    AND updated_at > (select MAX(updated_at) FROM trip_pro) - INTERVAL 30 day
                    GROUP BY user 
                    ORDER BY 1 DESC
            ), tmp_drop_off AS (
                    SELECT user, MAX(updated_at) as last_dop_off
                    FROM trip_pro 
                    WHERE reason = 'drop_off'
                    GROUP BY user
                    ORDER BY last_dop_off desc
             ), tmp_count_trip as (
                    SELECT count(*) as count, driver, user from trip_pro
                    WHERE reason = 'drop_off'
                    GROUP BY driver, user
                    ORDER BY 1 DESC
            ), tmp_max_count_trip as (
                    SELECT 
                    DISTINCT max(count) AS trips_frequent_driver, user, driver AS frequent_driver
                    FROM tmp_count_trip 
                    GROUP BY user
                    ORDER BY 1 DESC
                    ), tmp_avg_price_trip AS (
                    SELECT user, AVG(price) as avg_price_trip
                    FROM trip_pro
                    WHERE reason = 'drop_off'
                    GROUP BY user
                    ORDER BY 1 DESC
            )
            SELECT DISTINCT 
            a.user, IFNULL(b.count_30_days, 0) AS count_30_days, 
            IFNULL(c.count_15_days,0) AS count_15_days, 
            IFNULL(d.count_7_days,0) AS count_7_days, 
            a.last_dop_off, e.frequent_driver, e.trips_frequent_driver, f.avg_price_trip
            FROM tmp_drop_off a 
            LEFT JOIN tmp_30 b ON a.user = b.user
            LEFT JOIN tmp_15 c ON a.user = c.user
            LEFT JOIN tmp_7 d ON a.user = d.user
            LEFT JOIN tmp_max_count_trip e ON a.user = e.user
            LEFT JOIN tmp_avg_price_trip f ON a.user = f.user
            ORDER BY count_30_days DESC, count_15_days DESC, count_7_days DESC"""


def insert_stats_pro() -> str:
    return """REPLACE INTO stats_pro VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"""