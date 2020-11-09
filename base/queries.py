from elasticsearch_dsl import Q
from typing import List


class Query(object):
    """
    Query class contains different use-case raw queries.
    """
    @staticmethod
    def get_category_suggestion_query(category_name: str):
        category_suggestion_query = {
            "suggest": {
                "category_suggestion": {
                    "text": category_name,
                    "term": {
                        "field": "category"
                    }
                }
            }
        }
        return category_suggestion_query

    @staticmethod
    def get_product_by_name(product_name: str):
        product_query = {
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "product_name": product_name
                        }
                    }
                }
            }
        }
        return product_query

    @staticmethod
    def get_product_by_id(product_id: int):
        product_query = {
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "product_id": product_id
                        }
                    }
                }
            }
        }
        return product_query

    @staticmethod
    def get_multiple_products_by_ids(product_ids: List[int]):
        multiple_products_query = {
            "query": {
                "bool": {
                    "must": {
                        "terms": {
                            "product_id": product_ids
                        }
                    }
                }
            }
        }
        return multiple_products_query

    @staticmethod
    def boost_product_query(category_name: str, product_name_term: str):
        boost_query = {
            "query": {
                "function_score": {
                    "query": {
                        "bool": {
                            "should": [
                                {
                                    "match": {
                                        "category": category_name
                                    }
                                },
                                {
                                    "match": {
                                        "product_name": product_name_term
                                    }
                                }
                            ]
                        }
                    }
                },
                "boost": 5,
                "random_score": {},
                "boost_mode": "multiply"
            }
        }
        return boost_query


class DSLQuery(object):
    """
    DSLQuery defines different use-case queries using
    elasticsearch-dsl module.
    """

    def __init__(self, es_client, search):
        """

        :param es_client: Elasticsearch Client
        :param search: Search method instance of Elasticsearch client
        """
        self._es_client = es_client
        self._search = search

    @staticmethod
    def get_product_query(product_name):
        """
        This method illustrates the example of working of analyzers in ES.

        :param product_name: 'yellow and capsicum' example taken from slide.
        :return: document of 'yellow capsicum'
        """
        product_query = Q('bool',
                          must=[Q('bool', product_name=product_name)])
        return product_query

    @staticmethod
    def set_category_suggestion(
        self,
        suggestion_name: str,
        incorrect_word: str
    ):
        """
        This method illustrates the example of working of suggestions in ES

        :param self: Search method instance coming from Elasticsearch client
        :param suggestion_name:
        :param incorrect_word:
        :return:
        """
        config_suggestions = self._search.suggest(name=suggestion_name,
                                                  text=incorrect_word,
                                                  kwargs={'field': 'category'})
        return config_suggestions
