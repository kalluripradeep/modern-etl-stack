import urllib.request, json
url = "https://api.github.com/repos/apache/airflow/issues/64151"
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read())
        print(f"TITLE: {data['title']}")
        print(f"BODY: {data['body'][:1500]}")
except Exception as e:
    print(e)
