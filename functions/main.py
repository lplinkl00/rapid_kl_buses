# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`

from firebase_functions import https_fn
from firebase_functions.options import set_global_options
from firebase_admin import initialize_app

# For cost control, you can set the maximum number of containers that can be
# running at the same time. This helps mitigate the impact of unexpected
# traffic spikes by instead downgrading performance. This limit is a per-function
# limit. You can override the limit for each function using the max_instances
# parameter in the decorator, e.g. @https_fn.on_request(max_instances=5).
set_global_options(max_instances=10)

import folium
import pandas as pd
from google.transit import gtfs_realtime_pb2
from google.protobuf.json_format import MessageToDict
import requests


initialize_app()

@https_fn.on_request(cors = True)
def load_map_on_request(req: https_fn.Request) -> https_fn.Response:
    rapid_buses_url = "https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana?category=rapid-bus-kl"
    mrt_buses_url = "https://api.data.gov.my/gtfs-realtime/vehicle-position/prasarana?category=rapid-bus-mrtfeeder"
    ip_url = "https://ipinfo.io"

    #get current user location
    def get_usr_location(url):
        ipinfo = requests.get(url)
        usr_ip = ipinfo.json()
        usr_loc = usr_ip.get('loc')
        return usr_loc

    def get_gtfs(url):
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get(url)
        feed.ParseFromString(response.content)
        vehicle_positions = [MessageToDict(entity.vehicle) for entity in feed.entity]
        print(f'Total vehicles: {len(vehicle_positions)}')
        df = pd.json_normalize(vehicle_positions)
        return df

    rapid_bus = get_gtfs(rapid_buses_url)
    mrt_bus = get_gtfs(mrt_buses_url)
    m = folium.Map(location=[3.1319, 101.6841], zoom_level = 10, tiles="OpenStreetMap")
    usr_coords = get_usr_location(ip_url)
    bus_rpd_fg = folium.FeatureGroup(name="Rapid KL buses")
    bus_mrt_fg = folium.FeatureGroup(name="MRT Buses")
    bus_icon = folium.Icon(color="blue", icon="bus", prefix="fa")

    def get_bus_pos(df, fg):
        for s in range(len(df)):
            lat = df["position.latitude"].iloc[s]
            lon = df["position.longitude"].iloc[s]
            bus_id = df["vehicle.id"].iloc[s]
            folium.Marker(location=[lat, lon],icon=bus_icon, radius=4, popup=bus_id ).add_to(fg)
        return https_fn.Response("Hello world!")

    get_bus_pos(rapid_bus, bus_rpd_fg)
    get_bus_pos(mrt_bus, bus_mrt_fg)
    bus_mrt_fg.add_to(m)
    bus_rpd_fg.add_to(m)
    test_usr_location = [3.155687,101.692563]
    usr_fg = folium.FeatureGroup(name="User")
    usr_icon = folium.Icon(color="red", icon="person", prefix="fa")
    folium.Marker(location=test_usr_location, icon=usr_icon, radius=4).add_to(usr_fg)
    print(usr_coords.split(','))
    usr_fg.add_to(m)
    html = m.to_html()
    return https_fn.Response(html,
        headers={
            "Content-Type": "text/html" # or your specific host
        },)