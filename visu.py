#!/usr/bin/env python3
# coding: utf-8
"""
Visualisation of Velib data.

The generated maps shows the number of available bikes for each stations,
plotted on the map of Paris. We use a Voronoi diagram to draw tiles on top of
Paris map (think of each tiles as the "area of influence" of a given Velib
station). Green represents a large number of available bikes, red represents a
low number of available bikes.

This script requires a couple of arguments from the command-line:
    * The path to the SQLite DB file to use.
    * The path to the folder in which generated images should be put.
    * [Optional] A timestamp to start from, to resume operation for instance.

Note: This code does not take into account the stations events (change of
station size, new stations, stations deletion). Hence, there might be small
mistakes in the visualization, such as stations without data. This should be
handled in an improved version.
"""
from __future__ import division

import datetime
import logging
import os
import pickle
import sqlite3
import sys

import matplotlib
matplotlib.use('AGG')  # Use non-interactive backend
import matplotlib.pyplot as plt
import progressbar  # progressbar2 pip module
import smopy

from scipy.spatial import Voronoi, voronoi_plot_2d


def get_hue(percentage):
    """
    Convert a percentage to a hue,
    to map a percentage to a color
    in the green - yellow - orange - red scale.

    Red means 0%, green means 100%.
    """
    value = (100 - percentage) / 100.0
    hue = (1 - value) * 120
    return hue / 360.0


# Handle arguments from command-line
if len(sys.argv) < 3:
    sys.exit('Usage: %s db_file out_dir' % sys.argv[0])
db_file = sys.argv[1]
out_dir = sys.argv[2]

# Handle optional first timestamp argument
first_timestamp = None
if len(sys.argv) > 3:
    first_timestamp = int(sys.argv[3])

# Init progressbar and logging
progressbar.streams.wrap_stderr()  # Required before logging.basicConfig
logging.basicConfig(level=logging.INFO)

# Ensure out folder exists
if not os.path.isdir(out_dir):
    logging.info('Creating output folder %s…', out_dir)
    os.makedirs(out_dir)


# Load all stations from the database
logging.info('Loading all stations from the database…')
conn = sqlite3.connect(db_file)
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
voronoi_file = os.path.join(out_dir, 'voronoi.dat')
with open(voronoi_file, 'wb') as fh:
    pickle.dump(vor_regions, fh)
logging.info('Dumped Voronoi diagram to voronoi.dat')


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
if first_timestamp:
    time_data = c.execute(
        "SELECT DISTINCT updated FROM stationsstats WHERE updated > ? ORDER BY updated ASC",
        (first_timestamp,)
    )
else:
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
            # This can happen for a station at the boundaries (we volontarily
            # ignore them) or for station which disappeared at some point in
            # the dataset (as we don't handle stations events by now).
            logging.debug('Unknown Voronoi region for station %d.', station_data[0])

    # Output frame if necessary
    if t >= last_t + timesteps:
        ax.set_title(datetime.datetime.fromtimestamp(t // 1000).strftime('%d/%m/%Y %H:%M'))
        fig.tight_layout()
        fig.savefig(os.path.join(out_dir, '%d.png' % t))
        last_t = t

# Output last frame
fig.tight_layout()
fig.savefig(os.path.join(out_dir, '%d.png' % t))
