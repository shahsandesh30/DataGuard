import requests

API_KEY = "199e22399d61b1e767200a0ab07e86a6dd3c353fbfbe7a77023e528bc3c68f5b"
url = "https://api.openaq.org/v3/locations"
params = {
    "bbox": "149.977112,-34.364977,151.995850,-33.452068",  # minLon,minLat,maxLon,maxLat
    "limit": 1000,
    "page": 1,
}
headers = {"X-API-Key": API_KEY}

resp = requests.get(url, params=params, headers=headers)
data = resp.json()

sydney_locations = [
    {"id": loc["id"], "name": loc["name"], "provider": loc["provider"]["name"]}
    for loc in data["results"]
]
print(len(sydney_locations), "locations found")
print(sydney_locations, "locations found")