import psycopg2

from logger import Logger
from dataclasses import dataclass
from constants.app_constants import Database

logger = Logger.get_info_logger()


@dataclass
class DatabaseServer(object):
    """
    Gets data from the Postgres database server.
    """
    @staticmethod
    def connect_database_and_fetch_data(self):
        try:
            connection = psycopg2.connect(
                user=Database.DB_USER,
                password=Database.DB_PASSWORD,
                host=Database.DB_HOST,
                port=Database.DB_PORT,
                database=Database.DB_NAME
            )

            cursor = connection.cursor()
            postgres_select_query = "SELECT row_to_json(products) FROM products"  # NOQA

            # Index products data
            products_data = self._index_products_data(
                cursor=cursor,
                sql_query=postgres_select_query
            )

            # Index extra information from 'product_info' table
            # Extra information includes => brand_name, discount, etc
            postgres_select_query = "SELECT row_to_json(products_extra_info) FROM products_extra_info"  # NOQA

            products_extra_info = self._index_products_extra_info(
                cursor=cursor,
                sql_query=postgres_select_query
            )

            return products_data, products_extra_info

        except (Exception, psycopg2.Error) as exc:
            logger.info(
                "Error reported from PostgreSQL: {}".format(exc),
                exc_info=True
            )

        finally:
            # Close database connection
            if connection:
                connection.close()
                cursor.close()
                logger.info("Database connection is closed now.")

    @staticmethod
    def _index_products_data(cursor, sql_query: str):
        cursor.execute(sql_query)
        logger.info("Selecting rows from PRODUCTS table...")

        products_data = cursor.fetchAll()
        return products_data

    @staticmethod
    def _index_products_extra_info(cursor, sql_query: str):
        cursor.execute(sql_query)
        logger.info("Selecting rows from PRODUCTS_EXTRA_INFO table...")

        products_extra_info = cursor.fetchAll()
        return products_extra_info
