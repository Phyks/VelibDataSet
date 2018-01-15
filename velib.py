#!/usr/bin/env python3
from config import *

import datetime
import json
import os
import pybikes
import sqlite3
import time


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
    c.execute("CREATE TABLE IF NOT EXISTS stations(" +
              "id INTEGER, " +
              "name TEXT, " +
              "address TEXT, " +
              "latitude REAL, " +
              "longitude REAL, " +
              "banking INTEGER, " +
              "bonus INTEGER, " +
              "bike_stands INTEGER)")
    c.execute("CREATE TABLE IF NOT EXISTS stationsstats(" +
              "station_id INTEGER, " +
              "available_bikes INTEGER, " +
              "available_ebikes INTEGER, " +
              "free_stands INTEGER, " +
              "status TEXT, " +
              "updated INTEGER, " +
              "FOREIGN KEY(station_id) REFERENCES stations(id) ON DELETE CASCADE)")
    c.execute("CREATE TABLE IF NOT EXISTS stationsevents(" +
              "station_id INTEGER, " +
              "timestamp INTEGER, " +
              "event TEXT, " +
              "FOREIGN KEY(station_id) REFERENCES stations(id) ON DELETE CASCADE)")
    c.execute("CREATE INDEX IF NOT EXISTS stationstats_station_id ON stationsstats (station_id)");
    c.execute("CREATE INDEX IF NOT EXISTS stationsstats_updated ON stationsstats (updated)");
    c.execute("CREATE INDEX IF NOT EXISTS stationsevents_station_id ON stationsevents (station_id)");
    c.execute("CREATE INDEX IF NOT EXISTS stationsevents_timestamp ON stationsevents (timestamp)");
    conn.commit()
    return conn


def update_stations(conn):
    """
    Update the stored station list.

    :param conn: Database connection.
    """
    c = conn.cursor()
    database_stations = {i[0]: i
                         for i in
                         c.execute("SELECT id, name, address, latitude, longitude, banking, bonus, bike_stands FROM stations").fetchall()}

    velib = pybikes.get("velib")
    velib.update()
    for station in velib.stations:
        try:
            # Get old station entry if it exists
            old_station = database_stations[station.extra["uid"]]
            # Diff the two stations
            event = []
            if station.name != old_station[1]:
                event.append({"key": "name",
                              "old_value": old_station[1],
                              "new_value": station.name})
            if station.latitude != old_station[3]:
                event.append({"key": "latitude",
                              "old_value": old_station[3],
                              "new_value": station.latitude})
            if station.longitude != old_station[4]:
                event.append({"key": "longitude",
                              "old_value": old_station[4],
                              "new_value": station.longitude})
            if station.extra["banking"] != old_station[5]:
                event.append({"key": "banking",
                              "old_value": old_station[5],
                              "new_value": station.extra["banking"]})
            if station.extra["slots"] != old_station[7]:
                event.append({"key": "bike_stands",
                              "old_value": old_station[7],
                              "new_value": station.extra["slots"]})
            # If diff was found
            if len(event) > 0:
                # Update
                c.execute("UPDATE " +
                          "stations " +
                          "SET name=?, latitude=?, longitude=?, " +
                          "banking=?, bike_stands=? WHERE id=?",
                          (station.name,
                           station.latitude,
                           station.longitude,
                           station.extra["banking"],
                           station.extra["slots"],
                           station.extra["uid"]))
                # And insert event in the table
                c.execute("INSERT INTO " +
                          "stationsevents(station_id, timestamp, event) " +
                          "VALUES(?, ?, ?)",
                          (station.extra["uid"],
                           int(time.time()),
                           json.dumps(event)))
        except KeyError:
            c.execute("INSERT INTO " +
                      "stations(id, name, address, latitude, longitude, banking, bonus, bike_stands) " +
                      "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                      (station.extra["uid"],
                       station.name,
                       "",  # Not available
                       station.latitude,
                       station.longitude,
                       station.extra["banking"],
                       False,  # Not available
                       station.extra["slots"]))
        except TypeError:
            conn.rollback()
            return

        c.execute("INSERT INTO " +
                  "stationsstats(station_id, available_bikes, available_ebikes, free_stands, status, updated) " +
                  "VALUES(?, ?, ?, ?, ?, ?)",
                  (station.extra["uid"],
                   station.bikes - station.extra["ebikes"],
                   station.extra["ebikes"],
                   station.free,
                   station.extra["status"],
                   int(time.time())))  # Not available, using current timestamp
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
