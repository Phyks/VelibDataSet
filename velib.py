#!/usr/bin/env python3
from config import *

import json
import requests
import sqlite3
import time


def db_init():
    """
    Initialize a database connection, initialize the tables.

    Returns a new connection.
    """
    conn = sqlite3.connect("data.db")
    c = conn.cursor()
    # TODO: Init tables
    c.execute("CREATE TABLE IF NOT EXISTS stations(" +
              "id INTEGER PRIMARY KEY, " +
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
              "free_stands INTEGER, " +
              "status TEXT, " +
              "updated INTEGER, " +
              "FOREIGN KEY(station_id) REFERENCES stations(id) ON DELETE CASCADE)")
    c.execute("CREATE TABLE IF NOT EXISTS stationsevents(" +
              "station_id INTEGER, " +
              "timestamp INTEGER, " +
              "event TEXT, " +
              "FOREIGN KEY(station_id) REFERENCES stations(id) ON DELETE CASCADE)")
    c.execute("CREATE INDEX IF NOT EXISTS stationsevents_station_id ON stationsevents (station_id)");
    conn.commit()
    return conn


def retrieve_stations():
    """
    Retrieve list of stations.

    Returns the new stations list.
    """
    # Fetch the endpoint
    r = requests.get(api_endpoint,
                     params={"apiKey": api_key, "contract": contract})
    # Handle the JSON response
    stations_list_json = json.loads(r.text)
    stations_list = []
    for station in stations_list_json:
        stations_list.append(station)
    return stations_list


def update_stations():
    """
    Update the stored station list.
    """
    conn = db_init()
    c = conn.cursor()
    stations = retrieve_stations()
    database_stations = {i[0]: i
                         for i in
                         c.execute("SELECT id, name, address, latitude, longitude, banking, bonus, bike_stands FROM stations").fetchall()}
    for station in stations:
        try:
            # Get old station entry if it exists
            old_station = database_stations[station["number"]]
            # Diff the two stations
            event = []
            if station["name"] != old_station[1]:
                event.append({"key": "name",
                              "old_value": old_station[1],
                              "new_value": station["name"]})
            if station["address"] != old_station[2]:
                event.append({"key": "address",
                              "old_value": old_station[2],
                              "new_value": station["address"]})
            if station["position"]["lat"] != old_station[3]:
                event.append({"key": "latitude",
                              "old_value": old_station[3],
                              "new_value": station["position"]["lat"]})
            if station["position"]["lng"] != old_station[4]:
                event.append({"key": "longitude",
                              "old_value": old_station[4],
                              "new_value": station["position"]["lng"]})
            if station["banking"] != old_station[5]:
                event.append({"key": "banking",
                              "old_value": old_station[5],
                              "new_value": station["banking"]})
            if station["bonus"] != old_station[6]:
                event.append({"key": "bonus",
                              "old_value": old_station[6],
                              "new_value": station["bonus"]})
            if station["bike_stands"] != old_station[7]:
                event.append({"key": "bike_stands",
                              "old_value": old_station[7],
                              "new_value": station["bike_stands"]})
            # If diff was found
            if len(event) > 0:
                # Update
                c.execute("UPDATE " +
                          "stations " +
                          "SET name=?, address=?, latitude=?, longitude=?, " +
                          "banking=?, bonus=?, bike_stands=? WHERE id=?",
                          (station["name"],
                           station["address"],
                           station["position"]["lat"],
                           station["position"]["lng"],
                           station["banking"],
                           station["bonus"],
                           station["bike_stands"],
                           station["number"]))
                # And insert event in the table
                c.execute("INSERT INTO " +
                          "stationsevents(station_id, timestamp, event) " +
                          "VALUES(?, ?, ?)",
                          (station["number"],
                           time.time(),
                           json.dumps(event)))
        except KeyError:
            c.execute("INSERT INTO " +
                      "stations(id, name, address, latitude, longitude, banking, bonus, bike_stands) " +
                      "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                      (station["number"],
                       station["name"],
                       station["address"],
                       station["position"]["lat"],
                       station["position"]["lng"],
                       station["banking"],
                       station["bonus"],
                       station["bike_stands"]))
        except TypeError:
            conn.rollback()
            return

        c.execute("INSERT INTO " +
                  "stationsstats(station_id, available_bikes, free_stands, status, updated) " +
                  "VALUES(?, ?, ?, ?, ?)",
                  (station["number"],
                   station["available_bikes"],
                   station["available_bike_stands"],
                   station["status"],
                   station["last_update"]))
    conn.commit()


def main():
    """
    Handle main operations.
    """
    # Get updated list of stations
    update_stations()


if __name__ == "__main__":
    main()
