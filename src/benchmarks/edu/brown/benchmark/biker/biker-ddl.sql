-- create a table to hold stations
CREATE TABLE stations
(
  station_id    integer     NOT NULL
, location_text varchar(50) NOT NULL
, lat           float      NOT NULL
, lon           float      NOT NULL
, CONSTRAINT PK_stations PRIMARY KEY
  (
    station_id
  )
);

-- create a table to hold docks and info about them
CREATE TABLE docks
(
  dock_id    integer NOT NULL
, bike_id    integer
, station_id integer NOT NULL
, CONSTRAINT PK_docks PRIMARY KEY
  (
    dock_id
  )
);
  -- would like a constraint that said that bike_id cannot
  -- be null in the docks table if there is a bike reservation

CREATE TABLE reservations
(
  dock_id              integer NOT NULL
, is_bike_reservation  integer NOT NULL -- 1 if is a reservation for a bike
                                       -- 0 is if a reservation for a dock
, expiration_timestamp varchar(20) NOT NULL
, CONSTRAINT PK_reservations PRIMARY KEY -- only one reservation per dock at a time
 (
    dock_id
 )
);
