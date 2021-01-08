import requests
from time import sleep

query_id = 9536
query_parameters = {"from_block":"11138424","to_block":"11138430"}

payload = {"id":query_id,"parameters":query_parameters,"max_age":0}

headers = {
    "content-type": "application/json",
    "accept": "application/json"
}

jar = requests.cookies.RequestsCookieJar()

# TODO: get this cookie somehow (otherwise just paste it from the browser after authentication)
jar.set(
    "remember_token",
    "4837-1810fc3e62122c19ba4de7bf07428e5d|8f17242444d80d6cf1881b3accc8c4f5227004b53c66d84a13d795dcd8a811daca56766e66a5849e768d547f866a6dda816e1f7ad3468d962f5037c4126e39ba",
    domain="explore.duneanalytics.com",
    path="/"
)

# Send query
r = requests.post(f'https://explore.duneanalytics.com/api/queries/{query_id}/results', json=payload, headers=headers, cookies=jar)
job = r.json()["job"]

# Wait for job completion
while job["query_result_id"] is None:
    r = requests.get(f"https://explore.duneanalytics.com/api/jobs/{job['id']}", headers=headers, cookies=jar)
    job = r.json()["job"]
    sleep(5)

# Get results 
query_result_id = job["query_result_id"]
r = requests.get(f'https://explore.duneanalytics.com/api/query_results/{query_result_id}', headers=headers, cookies=jar)
print(r.json())
