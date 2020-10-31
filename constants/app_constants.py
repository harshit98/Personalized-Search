from enum import Enum


class Search(Enum):
    ES_INDEX = "products"
    ES_HOST = "http://localhost:9200"
    ES_INDEX_PRODUCTS_NEW = "products_new"


class Database(Enum):
    DB_NAME = "postgres"
    DB_HOST = "http://localhost"
    DB_PORT = 5432
    DB_USER = "root"
    DB_PASSWORD = "password"
