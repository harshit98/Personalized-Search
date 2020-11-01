from base.search import PersonalizedSearch
from constants.app_constants import Search
from database.connection import DatabaseServer
from elasticsearch.exceptions import TransportError
from logger import Logger

from base.process import get_processed_json_data

logger = Logger.get_info_logger()


def start_search_app():
    search = PersonalizedSearch()
    es_client = search.connect_elasticsearch()

    # Get products_data and products_extra_info
    products_data, products_extra_info = DatabaseServer.connect_database_and_fetch_data()  # NOQA

    mappings = search.create_mappings()

    # Create new index in the Elasticsearch
    es_client.indices.create(index=Search.ES_INDEX_PRODUCTS_NEW,
                             body=mappings)

    processed_data = get_processed_json_data(products_data, products_extra_info)
    # Bulk process
    try:
        response = es_client.bulk(body=processed_data,
                                  index=Search.ES_INDEX_PRODUCTS_NEW)

        if response:
            logger.info("Data has been indexed successfully!")

    except (Exception, TransportError) as exc:
        logger.info("Error in data indexing: {}".format(exc), exc_info=True)


if __name__ == "__main__":
    start_search_app()
