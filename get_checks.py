import urllib.request, json
url = "https://api.github.com/repos/apache/airflow/commits/b4499c8be558fbc8f731e36abc5db63c47b5bdf3/check-runs"
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read())
        for run in data.get('check_runs', []):
            if run['conclusion'] == 'failure':
                print(f"FAILED: {run['name']}")
                print(f"URL: {run['html_url']}")
except Exception as e:
    print(e)
