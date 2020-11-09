from logger import Logger

logger = Logger.get_info_logger()


def get_processed_json_data(products_data, products_extra_info):
    """
    Appends the data of 'products_extra_info' table to product_data' table.

    :param products_data: Product Data
    :param products_extra_info: Extra information like brand_name, discount.
    :return: Aggregated data.
    """
    start = 0

    for product_data in products_data:
        product_data["products_extra_info"] = {}

        for i in range(start=start, stop=len(products_extra_info)):
            products_data["products_extra_info"] = products_extra_info[i]
            start += 1
            break

    return products_data
