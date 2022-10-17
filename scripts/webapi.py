from firebase_config import config
import requests

def post(endpoint, data, *parameters):
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + config["apiKey"],
        'Accept': 'application/json'
    }
    url = config["api-url"] + "/" + endpoint
    if len(parameters) > 0:
        url += "?"
        for p in parameters:
            url += f"{p}&"

    r = requests.post(url, headers=headers, json=data)
    print(r)
    print(f"Response: {r.text}")