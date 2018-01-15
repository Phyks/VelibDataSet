VelibDataSet
============

This code can be used to dump periodically all the available data from the
Velib API, the bike sharing system in Paris.

It is basically a wrapper around [pybikes](https://github.com/eskerda/pybikes)
to dump values periodically in a SQLite database.


## Usage

* Clone this repo.
* Install [pybikes](https://github.com/eskerda/pybikes).
* Run `python2 velib.py`.

_Note:_ For now, `pybikes` is only Python2 compatible.


## Dumped data

**Important:** For the latest information about the dump available at
https://pub.phyks.me/datasets/velib/, please refer to
https://pub.phyks.me/datasets/velib/README.txt.

This script is used to dump the returned data from the Velib API every few
minutes. Dumps are available at https://pub.phyks.me/datasets/velib/.

The script writes in a new SQLite file every week, put in a different folder
by year, and labelled with the week number.

Each SQLite file has three tables:

* A `stations` table, containing "permanent" information about each station
  (latitude, longitude, number of stands etc).
* A `stationsstats` table which contains the available number of bikes and
  stands at each time, for each station. Not that these data are directly
  dumped from the API, hence `updated` field is coming from the API and is a
  timestamp in milliseconds.
* A `stationsevents` table keeps tracks of modifications of fields in the
  `stations` table. For instance when a mobile station changes position,
  `latitude` and `longitude` are updated, or when a station gains new
  `stands`, this table keeps track of the changes.

You should have a look at the `init_db` function (or run `.schema` in the
resulting SQLite database) to have more details about the structure of these
tables, it should be rather self-explicit.

_Note_: There are currently no ways to explicitly list stations addition /
removal. As the API response always contains the data for all the available
stations, you can find when a station was created (removed) by looking at the
first (last) time a line was added in `stationsstats` table for this station.


## Visualization

The visualization script generates sequences of PNG images from your database
dump. You can then concatenate them in a `x264` movie using `ffmpeg` (or
`avconv`, should be the same command):

```
cat *.png | ffmpeg -f image2pipe -framerate 10 -i - output.mp4
```


## Links

* Velib website: http://velib-metropole.fr/

## License

Code is released under MIT license.
