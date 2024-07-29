import math
import requests

# Authentication
username = '__key__'
password = ''    # API key

json_headers = {
            "username": username,
            "password": password,
            "Content-Type": "application/json",
        }

# Remember that the time series information is found in the .../timeseries/{uuid}/events/ page.
uuid = '3d1a01b8-1338-474c-9d4e-21f614686c5a'

start_timeseries_events_url = 'https://hdsr.lizard.net/api/v4/timeseries/{}/events/?value__isnull=False'.format(uuid)

#Retrieve the 'results' attribute using a JSON interpreter
data = requests.get(start_timeseries_events_url,headers=json_headers)

pages = math.ceil(data.json()['count'] / 10)
results = data.json()['results']
start = results[0]['time']

end_timeseries_events_url = 'https://hdsr.lizard.net/api/v4/timeseries/4b0db57b-995b-4b23-bdc1-0c0e5911a187/events/?page={}&value__isnull=False'.format(pages)

data_end = requests.get(end_timeseries_events_url, headers= json_headers)
results_end = data_end.json()['results']
end = results_end[-1]['time']

print("Start time: ", start,
      "\nEnd time: ", end)