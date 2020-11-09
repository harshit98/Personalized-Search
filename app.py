from base.search import PersonalizedSearch
from base.queries import Query, DSLQuery
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
    products_data, products_extra_info = \
        DatabaseServer.connect_database_and_fetch_data()

    mappings = search.create_mappings()

    # Create new index in the Elasticsearch
    es_client.indices.create(index=Search.ES_INDEX_PRODUCTS_NEW,
                             body=mappings)

    processed_data = get_processed_json_data(
        products_data=products_data,
        products_extra_info=products_extra_info
    )

    # Bulk process
    try:
        response = es_client.bulk(body=processed_data,
                                  index=Search.ES_INDEX_PRODUCTS_NEW)

        if response:
            logger.info("Data has been indexed successfully!")

    except (Exception, TransportError) as exc:
        logger.info("Error in data indexing: {}".format(exc), exc_info=True)

    # Query product by name
    product_info = Query.get_product_by_name('spicy chips')

    # Using DSLQuery class
    # results = DSLQuery.get_product_query('spicy chips')

    logger.info(f'{product_info}')

    # Query products by id
    products_info = Query.get_multiple_products_by_ids(product_ids=[1, 2])
    logger.info(f'{products_info}')

    # Boost product 'spicy chips' and get results
    results = Query.boost_product_query(category_name='snacks',
                                        product_name_term='spicy')

    for hits in results['hits']['hits']:
        logger.info(f'{hits}')


if __name__ == "__main__":
    start_search_app()
