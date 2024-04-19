import os

from dotenv import load_dotenv
from manticoresearch import Configuration

load_dotenv()


class SearchConfig:
    def __init__(self):
        self.host = os.getenv('SEARCH_HOST') or None
        self.username = os.getenv('SEARCH_USERNAME') or None
        self.password = os.getenv('SEARCH_PASSWORD') or None
        self.discard_unknown_keys = os.getenv('DISCARD_UNKNOWN_KEYS') or False

    def get_config(self):
        return Configuration(
            host=self.host,
            username=self.username,
            password=self.password,
            discard_unknown_keys=self.discard_unknown_keys,
        )
