from dagster import resource
import responses 
import requests
import pandas as pd
import numpy as np
import random

class RawDataAPI():
    def __init__(self):
        pass

    @responses.activate
    def get_order_details(_, order_id):
        responses.get(
            # fake endpoint
            "http://api.jaffleshop.co/v1/order_details",

            # random order data returned, see utils.py
            json = pd.DataFrame({
                "order_id": [order_id],
                "order_center": [random.choices(["scranton", "albany", "new york"], k=1)]
            }).to_json()
        )

        return requests.get("http://api.jaffleshop.co/v1/order_details", params={"order_id": order_id})
    

@resource
def data_api(_):
    return RawDataAPI()    