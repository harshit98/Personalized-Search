import json

from logger import Logger
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
        index_settings = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "analysis": {
                    "filter": {
                        "filter_whitespace_remove": {
                            "type": "pattern_replace",
                            "pattern": " ",
                            "replacement": ""
                        },
                        "english_stop": {
                            "type": "stop",
                            "stopwords": "_english_"
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
                        },
                        "analyzer_remove_stopwords": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "english_stop"]
                        }
                    }
                }
            }
        }
        mapping = {
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

        index_settings = json.loads(index_settings)
        mapping = json.loads(mapping)

        merged_dict = {
            key: value
            for (key, value) in (index_settings.items() + mapping.items())
        }

        return json.dumps(merged_dict)
