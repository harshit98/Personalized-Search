from dataclasses import dataclass


@dataclass
class ProductData(object):
    """
    Stores product information as an object.
    """
    product_id: int
    product_name: str
    category: str
    price: float

    def get_product_id(self):
        return self.product_id

    def get_product_name(self):
        return self.product_name

    def get_product_category(self):
        return self.category

    def get_product_price(self):
        return self.price
