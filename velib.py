#!/usr/bin/env python3
import datetime
import json
import logging
import os
import requests
import sqlite3
import time

# Constants
VELIB_STATIONS_INFOS_URL = 'https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json'
VELIB_STATIONS_STATUS_URL = 'https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json'

# Set up logging
level = logging.WARNING
if os.environ.get('DEBUG', False):
    level = logging.DEBUG
logging.basicConfig(level=level)


def db_init(db_name=None):
    """
    Initialize a database connection, initialize the tables.

    Returns a new connection.
    """
    now = datetime.datetime.now()
    if db_name is None:
        db_name = "week_%s.db" % now.strftime("%V")
    db_folder = os.path.join(
        'data',
        now.strftime('%Y')
    )
    if not os.path.isdir(db_folder):
        os.makedirs(db_folder)

    conn = sqlite3.connect(os.path.join(db_folder, db_name))
    c = conn.cursor()
    # Init tables
    c.execute("""
        CREATE TABLE IF NOT EXISTS stations(
          id INTEGER,
          name TEXT,
          address TEXT,
          latitude REAL,
          longitude REAL,
          banking INTEGER,
          bonus INTEGER,
          bike_stands INTEGER
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS stationsstats(
          station_id INTEGER,
          available_bikes INTEGER,
          available_ebikes INTEGER,
          free_stands INTEGER,
          status TEXT,
          updated INTEGER,
          FOREIGN KEY(station_id) REFERENCES stations(id) ON DELETE CASCADE
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS stationsevents(
          station_id INTEGER,
          timestamp INTEGER,
          event TEXT,
          FOREIGN KEY(station_id) REFERENCES stations(id) ON DELETE CASCADE
        )
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS
          stationstats_station_id ON stationsstats (station_id)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS
          stationsstats_updated ON stationsstats (updated)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS
          stationsevents_station_id ON stationsevents (station_id)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS
          stationsevents_timestamp ON stationsevents (timestamp)
    """)
    conn.commit()
    return conn


def update_stations(conn):
    """
    Update the stored station list.

    :param conn: Database connection.
    """
    c = conn.cursor()

    logging.info('Get all stations from database...')
    database_stations = {
        i[0]: i
        for i in c.execute("""
            SELECT
              id,
              name,
              address,
              latitude,
              longitude,
              banking,
              bonus,
              bike_stands
            FROM stations
        """).fetchall()
    }

    logging.info('Get updated Velib stations from API...')
    req_stations = requests.get(VELIB_STATIONS_INFOS_URL)
    stations = {
        station['stationCode']: station
        for station in req_stations.json()['data']['stations']
    }
    req_status = requests.get(VELIB_STATIONS_STATUS_URL)

    # List of SQL queries to perform for
    events = []  # events happening on stations (temporary closure etc)
    stations_update = []  # Update of stations (such as new stands number)
    new_stations = []  # New stations to add to the list
    stats = []  # Current stats of the station

    logging.info('Processing fetched stations')
    for station in req_status.json()['data']['stations']:
        uid = station["stationCode"]
        try:
            # Get old station entry if it exists
            old_station = database_stations[uid]
            # Diff the two stations
            event = []
            if stations[uid]['name'] != old_station[1]:
                event.append({"key": "name",
                              "old_value": old_station[1],
                              "new_value": stations[uid]['name']})
            if stations[uid]['latitude'] != old_station[3]:
                event.append({"key": "latitude",
                              "old_value": old_station[3],
                              "new_value": stations[uid]['lat']})
            if stations[uid]['lon'] != old_station[4]:
                event.append({"key": "longitude",
                              "old_value": old_station[4],
                              "new_value": station[uid]['lon']})
            if station["numDocksAvailable"] != old_station[7]:
                event.append({"key": "bike_stands",
                              "old_value": old_station[7],
                              "new_value": stations[uid]["capacity"]})
            # If diff was found
            if len(event) > 0:
                stations_update.append(
                    (
                        stations[uid]['name'],
                        stations[uid]['lat'],
                        stations[uid]['lon'],
                        None,
                        stations[uid]['capacity'],
                        uid
                    )
                )
                events.append(
                    (
                        uid,
                        int(time.time()),
                        json.dumps(event)
                    )
                )
        except KeyError:
            # Station is unknown, add it
            new_stations.append(
                (
                    uid,
                    stations[uid]['name'],
                    "",  # Not available
                    stations[uid]['lat'],
                    stations[uid]['lon'],
                    None,  # Not available
                    False,  # Not available
                    stations[uid]["capacity"]
                )
            )

        # Add stats
        numEBikesAvailable = (
            station['numBikesAvailable']
            - next(
                x['ebike']
                for x in station['num_bikes_available_types']
                if 'ebike' in x
            )
        )
        stats.append(
            (
                uid,
                station['numBikesAvailable'],
                numEBikesAvailable,
                station['numDocksAvailable'],
                None,
                int(time.time())  # Not available, using current timestamp
            )
        )

    # Update stations
    logging.info('Updating stations in db...')
    c.executemany(
        """
        UPDATE
            stations
        SET
            name=?, latitude=?, longitude=?,
            banking=?, bike_stands=?
        WHERE id=?
        """,
        stations_update
    )

    # Insert events in the table
    logging.info('Insert stations events in db...')
    c.executemany(
        """
        INSERT INTO
          stationsevents(station_id, timestamp, event)
        VALUES(?, ?, ?)
        """,
        events
    )

    # Add the missing stations to database
    logging.info('Insert missing stations in db...')
    c.executemany(
        """
        INSERT INTO
        stations(
          id,
          name,
          address,
          latitude,
          longitude,
          banking,
          bonus,
          bike_stands
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?)
        """,
        new_stations
    )

    # Insert stats in database
    logging.info('Insert stations stats in db...')
    c.executemany(
        """
        INSERT INTO
        stationsstats(
          station_id,
          available_bikes,
          available_ebikes,
          free_stands,
          status,
          updated
        )
        VALUES(?, ?, ?, ?, ?, ?)
        """,
        stats
    )

    # Commit
    conn.commit()


def main():
    """
    Handle main operations.
    """
    # Get updated list of stations for smovengo
    conn = db_init()
    update_stations(conn)


if __name__ == "__main__":
    main()
