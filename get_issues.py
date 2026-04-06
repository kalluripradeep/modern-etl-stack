import urllib.request, json
url = "https://api.github.com/repos/apache/airflow/issues?labels=bug&state=open&sort=created&direction=desc&per_page=15"
req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
try:
    with urllib.request.urlopen(req) as response:
        data = json.loads(response.read())
        for issue in data:
            if 'pull_request' in issue:
                continue
            labels = [l['name'] for l in issue['labels']]
            print(f"#{issue['number']}: {issue['title']}\nLabels: {', '.join(labels)}\nComments: {issue['comments']}\n")
except Exception as e:
    print(e)
