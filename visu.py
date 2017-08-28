#!/usr/bin/env python3
# coding: utf-8
"""
Visualisation of Velib data

Note: This does not take into account the stations events.
"""
from __future__ import division

import datetime
import logging
import os
import pickle
import sqlite3

import matplotlib
matplotlib.use('AGG')  # Use non-interactive backend
import matplotlib.pyplot as plt
import progressbar
import smopy

from scipy.spatial import Voronoi, voronoi_plot_2d


def get_hue(percentage):
    """
    Convert a percentage to a hue,
    to map a percentage to a color
    in the green - yellow - orange - red scale.
    """
    value = percentage / 100.0
    hue = (1 - value) * 120
    return hue / 360.0


progressbar.streams.wrap_stderr()
logging.basicConfig(level=logging.INFO)

# Ensure out folder exists
if not os.path.isdir('out'):
    logging.info('Creating out folder…')
    os.mkdir('out')


# Load all stations from the database
logging.info('Loading all stations from the database…')
conn = sqlite3.connect("data.db")
c = conn.cursor()
stations = c.execute(
    "SELECT id, latitude, longitude, bike_stands, name FROM stations"
).fetchall()
stations = [
    station
    for station in stations
    if station[1] > 0 and station[2] > 0
]  # Filter out invalid stations
logging.info('Loaded %d stations from database.', len(stations))


# Set tiles server and params
smopy.TILE_SERVER = "http://a.tile.stamen.com/toner-lite/{z}/{x}/{y}@2x.png"
smopy.TILE_SIZE = 512
smopy.MAXTILES = 25

# Compute map bounds as the extreme stations
lower_left_corner = (
    min(station[1] for station in stations),
    min(station[2] for station in stations)
)
upper_right_corner = (
    max(station[1] for station in stations),
    max(station[2] for station in stations)
)

# Get the tiles
# Note: Force zoom to 12 to still have city names (and not e.g. street names)
logging.info('Fetching tiles between %s and %s…' % (lower_left_corner, upper_right_corner))
map = smopy.Map(lower_left_corner + upper_right_corner, z=12)


# Compute the station points coordinates
# (converting from lat/lng to pixels for matplotlib)
station_points = [
    map.to_pixels(station[1], station[2]) for station in stations
]

# Compute Voronoi diagram of available stations
logging.info('Computing Voronoi diagram of the stations…')
vor = Voronoi(station_points)
# This is a mapping between ID of stations and
# matching Voronoi tile, for faster reuse
vor_regions = {}
for point_index, region_index in enumerate(vor.point_region):
    station_id = stations[point_index][0]
    region = vor.regions[region_index]
    if -1 in region:  # Discard regions with points out of bounds
        continue
    vor_regions[station_id] = {
        "polygon": [vor.vertices[i] for i in region],  # Polygon, as a list of points
        "mpl_surface": None  # Will store the drawn matplotlib surface (to update it easily)
    }
# Dumping Voronoi diagram
with open('out/voronoi.dat', 'wb') as fh:
    pickle.dump(vor_regions, fh)
logging.info('Dumped Voronoi diagram to out/voronoi.dat')


# Plotting
logging.info('Initializing Matplotlib figure…')
map_img = map.to_pil()
aspect_ratio = map_img.size[1] / map_img.size[0]

# Create a matplotlib figure
fig, ax = plt.subplots(figsize=(8, 8 * aspect_ratio))
ax.set_xticks([])
ax.set_yticks([])
ax.grid(False)
# Compute bounds
# Note: This is necessary because OSM tiles have some spatial
# extension and might expand farther than the requested bounds.
x_min, y_min = map.to_pixels(lower_left_corner)
x_max, y_max = map.to_pixels(upper_right_corner)
ax.set_xlim(x_min, x_max)
ax.set_ylim(y_min, y_max)
ax.imshow(map_img)

# Initialize Voronoi Matplotlib surfaces to grey
logging.info('Initializing Voronoi surfaces in the figure…')
for station_id, region in vor_regions.items():
    vor_regions[station_id]["mpl_surface"] = ax.fill(
        alpha=0.25,
        *zip(*region["polygon"]),
        color="#9e9e9e"
    )[0]

# Get time steps
logging.info('Loading time steps from the database.')
time_data = c.execute(
    "SELECT DISTINCT updated FROM stationsstats WHERE updated ORDER BY updated ASC"
)
last_t = None
timesteps = 5 * 60 * 1000  # 5 mins timesteps between each frames

logging.info('Plotting graphs!')
bar = progressbar.ProgressBar()
for t, in bar(time_data):
    if last_t is None:
        # Initialize last_t
        last_t = t

    # For each available station, retrieve its time data
    c2 = conn.cursor()
    stations_stats = c2.execute(
        "SELECT station_id, available_bikes FROM stationsstats WHERE updated=?",
        (t,)
    )

    for station_data in stations_stats:
        # Compute the available bikes percentages for this station over time
        bike_stands = next(station[3] for station in stations if station[0] == station_data[0])
        percentage = station_data[1] / bike_stands * 100.0
        if percentage > 100:
            # TODO: This happens when a station has changed size inside the
            # dataset. Should be handled better.
            percentage = 100
        # Plot "regions of influence" of the velib stations (Voronoi regions)
        try:
            region = vor_regions[station_data[0]]
            region["mpl_surface"].set_color(matplotlib.colors.hsv_to_rgb([get_hue(percentage), 1.0, 1.0]))
        except KeyError:
            logging.warn('Unknown Voronoi region for station %d.', station_data[0])

    # Output frame if necessary
    if t >= last_t + timesteps:
        ax.set_title(datetime.datetime.fromtimestamp(t // 1000).strftime('%d/%m/%Y %H:%M'))
        fig.tight_layout()
        fig.savefig('out/%d.png' % t)
        last_t = t

# Output last frame
fig.tight_layout()
fig.savefig('out/%d.png' % t)
