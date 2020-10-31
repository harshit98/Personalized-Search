from logger import Logger

logger = Logger.get_info_logger()


def get_processed_json_data(products_data, products_extra_info):
    start = 0

    for product_data in products_data:
        product_data["products_extra_info"] = {}

        for i in range(start=start, stop=len(products_extra_info)):
            products_data["products_extra_info"] = products_extra_info[i]
            start += 1
            break

    return products_data
