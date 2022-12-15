import random

import numpy as np
import pandas as pd
import requests
import responses
from dagster import resource


class RawDataAPI:
    """Represents a mock data enrichment API"""

    def __init__(self):
        pass

    @responses.activate
    def get_order_details(_, order_id):
        responses.get(
            # fake endpoint
            "http://api.jaffleshop.co/v1/order_details",
            # adds an order center
            json=pd.DataFrame(
                {
                    "order_id": [order_id],
                    "order_center": [
                        random.choices(["scranton", "albany", "new york"], k=1)
                    ],
                }
            ).to_json(),
        )

        return requests.get(
            "http://api.jaffleshop.co/v1/order_details", params={"order_id": order_id}
        )


@resource
def data_api(_):
    """Represents a mock data enrichment API"""
    return RawDataAPI()
