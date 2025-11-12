import requests
import time
import os
import json
from dotenv import load_dotenv

load_dotenv()

BITSKINS_API_KEY = os.getenv("BITSKINS_API_KEY")

class BitSkinsClient:
    def __init__(self, amount_skins_unload: int = 10_000, api_key: str = BITSKINS_API_KEY):
        print("BITSKINS_API_KEY length:", len(api_key) if api_key else 0)
        self.api_key = api_key
        self.amount_skins_unload = amount_skins_unload
        self.fields_to_unload = ['id', 'price', 'sticker_counter', 'created_at', 'name', 'skin_id']
        self.url_api = "https://api.bitskins.com"

    def get_items(self):
        bd = []
        time_for_req = 1
        for i in range(1, self.amount_skins_unload):
            data = {
                "limit": 100,
                "offset": 0,
                "where": {"skin_id": [i]},
                "order": [{"field": "price", "order": "ASC"}],
            }

            headers = {'x-apikey': self.api_key}
            t = time.time()
            res = requests.post(url=f'{self.url_api}/market/search/730', headers=headers, json=data)
            res.raise_for_status()
            response = res.json()

            if "list" not in response:
                raise Exception("Что-то пошло не так, в ответе сервиса нет 'list'")

            bd += [
                {key: item[key] for key in self.fields_to_unload}
                for item in response['list']
            ]
            realised_time = time.time() - t
            time.sleep(max(time_for_req - realised_time, 0))

        return bd