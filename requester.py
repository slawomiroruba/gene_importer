import time
import requests
from bs4 import BeautifulSoup

class Requester:
    def __init__(self):
        self._request = None

    def send_request(self, request):
        self._request = request

    def get_response(self):
        return self._request.get_response()

    def fetch_data(url, params, headers, max_retries=20):
        for attempt in range(max_retries):
            response = requests.get(url, params=params, headers=headers)
            if response.status_code == 200:
                return response.json()  # returns the response as a Python dictionary
            else:
                print(f"Failed to fetch data. Status code: {response.status_code}. Attempt: {attempt + 1}")
                time.sleep(1)  # sleep for 1 second before next attempt
        print(f"Failed to fetch data after {max_retries} attempts.")
        return None

    def get_soup(self, url):
        return self.soup_parse(self.http_request(url))

    def http_request(url, params=None):
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response

    def soup_parse(response):
        soup = BeautifulSoup(response.text, 'html.parser')
        return soup