CREATE TABLE example.text (
  text STRING,
  start_time FLOAT64,
  end_time FLOAT64,
  confidence FLOAT64,
  time_offset FLOAT64,
  vertices ARRAY<STRUCT<x FLOAT64, y FLOAT64>>,
  video STRING
);
