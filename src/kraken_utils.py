# %%
import time
import os
import requests

import urllib.parse
import hashlib
import hmac
import base64

class Kraken_API():

    def __init__(self, api_key, api_sec):
        self.api_key = api_key
        self.api_sec = api_sec
        self.baseURL = "https://api.kraken.com"

    def get_kraken_signature(self, urlpath, data, secret):

        postdata = urllib.parse.urlencode(data)
        encoded = (str(data['nonce']) + postdata).encode()
        message = urlpath.encode() + hashlib.sha256(encoded).digest()

        mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
        sigdigest = base64.b64encode(mac.digest())
        return sigdigest.decode()

    # Attaches auth headers and returns results of a POST request
    def kraken_request(self, uri_path, data, api_key, api_sec):
        headers = {}
        headers['API-Key'] = api_key
        # get_kraken_signature() as defined in the 'Authentication' section
        headers['API-Sign'] = self.get_kraken_signature(uri_path, data, api_sec)             
        req = requests.post((self.baseURL + uri_path), headers=headers, data=data)
        assert req.status_code == 200, f'Request Failure: {req.status_code}, {req.reason}'
        
        return req

    def get_balance(self):
        # Construct the request and print the result
        resp = self.kraken_request('/0/private/Balance', {
            "nonce": str(int(1000*time.time()))
        }, self.api_key, self.api_sec)

        return resp

    def get_trade_balance(self):
        resp = self.kraken_request('/0/private/TradeBalance', {
            "nonce": str(int(1000*time.time())),
            "asset": "USD"
        }, self.api_key, self.api_sec)

        return resp
# %%
