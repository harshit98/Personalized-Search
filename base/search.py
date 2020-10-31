from logger import Logger
from typing import List

from constants.app_constants import Search
from dataclasses import dataclass

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError, ConnectionError

logger = Logger.get_info_logger()


@dataclass
class PersonalizedSearch(object):
    """
    Personalized Search will interact with Elasticsearch.
    """

    @staticmethod
    def connect_elasticsearch() -> Elasticsearch:
        """
        :return: Elasticsearch client.
        """
        try:
            es_client = Elasticsearch(hosts=Search.ES_HOST)
            return es_client

        except (TransportError, ConnectionError) as exc:
            logger.info(exc, exc_info=True)

    @staticmethod
    def create_mappings():
        mapping = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                # Custom analyzer for analyzing fields
                "analysis": {
                    "filter": {
                        "filter_whitespace_remove": {
                            "type": "pattern_replace",
                            "pattern": " ",
                            "replacement": ""
                        }
                    },
                    "analyzer": {
                        "analyzer_keyword_lowercase": {
                            "tokenizer": "keyword",
                            "filter": ["lowercase"]
                        },
                        "analyzer_remove_whitespace": {
                            "type": "custom",
                            "tokenizer": "keyword",
                            "filter": ["lowercase", "filter_whitespace_remove"]
                        }
                    }
                }
            },
            "mappings": {
                "properties": {
                    "category": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    },
                    "price": {
                        "type": "float"
                    },
                    "product_id": {
                        "type": "long"
                    },
                    "product_extra_info": {
                        "type": "nested",
                        "properties": {
                            "brand_name": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword",
                                        "ignore_above": 256
                                    }
                                }
                            },
                            "discount": {
                                "type": "float"
                            }
                        }
                    },
                    "product_name": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        }
                    }
                }
            }
        }
        return mapping

    @staticmethod
    def get_spellcheck_query(product_name_text: str):
        spellcheck_query = {
            "suggest": {
                "product_name_suggester": {
                    "text": product_name_text,
                    "term": {
                        "field": "product_name"
                    }
                }
            }
        }
        return spellcheck_query

    @staticmethod
    def fetch_single_product(product_id: int):
        fetch_single_product_query = {
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
        return fetch_single_product_query

    @staticmethod
    def fetch_multiple_products(product_ids: List[int]):
        fetch_multiple_products_query = {
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
        return fetch_multiple_products_query

    @staticmethod
    def fetch_aggregated_products():
        aggregation_query = {
            "aggs": {
                "list_of_products": {
                    "terms": {"field": "product_name"}
                }
            },
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": "now-1d/d",
                        "lt": "now/d"
                    }
                }
            }
        }
        return aggregation_query
