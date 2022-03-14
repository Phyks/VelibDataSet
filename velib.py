#!/usr/bin/env python3
import datetime
import json
import os
import requests
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

    req_stations = requests.get('https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_information.json')
    stations = {
        station['stationCode']: station
        for station in req_stations.json()['data']['stations']
    }
    req_status = requests.get('https://velib-metropole-opendata.smoove.pro/opendata/Velib_Metropole/station_status.json')
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
                # Update
                c.execute("UPDATE " +
                          "stations " +
                          "SET name=?, latitude=?, longitude=?, " +
                          "banking=?, bike_stands=? WHERE id=?",
                          (stations[uid]['name'],
                           stations[uid]['lat'],
                           stations[uid]['lon'],
                           None,
                           stations[uid]['capacity'],
                           uid))
                # And insert event in the table
                c.execute("INSERT INTO " +
                          "stationsevents(station_id, timestamp, event) " +
                          "VALUES(?, ?, ?)",
                          (uid,
                           int(time.time()),
                           json.dumps(event)))
        except KeyError:
            c.execute("INSERT INTO " +
                      "stations(id, name, address, latitude, longitude, banking, bonus, bike_stands) " +
                      "VALUES(?, ?, ?, ?, ?, ?, ?, ?)",
                      (uid,
                       stations[uid]['name'],
                       "",  # Not available
                       stations[uid]['lat'],
                       stations[uid]['lon'],
                       None,  # Not available
                       False,  # Not available
                       stations[uid]["capacity"]))
        except TypeError:
            conn.rollback()
            return

        numEBikesAvailable = (
            station['numBikesAvailable']
            - next(
                x['ebike']
                for x in station['num_bikes_available_types']
                if 'ebike' in x
            )
        )
        c.execute("INSERT INTO " +
                  "stationsstats(station_id, available_bikes, available_ebikes, free_stands, status, updated) " +
                  "VALUES(?, ?, ?, ?, ?, ?)",
                  (uid,
                   station['numBikesAvailable'],
                   numEBikesAvailable,
                   station['numDocksAvailable'],
                   None,
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
