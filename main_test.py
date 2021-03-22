d = {
    "token" : "c98arf53-ae39-4c9d-af44-c6957ee2f748",
    "customer": "Customer1",
    "content": "channel1",
    "timespan": 30000,
    "p2p": 560065,
    "cdn": 321123,
    "sessionDuration": 120000,
}
import requests
import json
import time
#Replace the post request
while True :
    requests.post("http://[LOCAL_HOST||IP]:5000/stats", json=d)
    time.sleep(30)
