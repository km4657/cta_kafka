"""Defines trends calculations for stations"""
import logging

import faust
from dataclasses import dataclass



logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
@dataclass
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
@dataclass
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.

app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("connect-stations", partitions=None,value_type=Station)
# TODO: Define the output Kafka Topic
out_topic = app.topic("connect-stations.transformed", partitions=1,key_type=str,value_type=TransformedStation)
# TODO: Define a Faust Table
stations_summary_table = app.Table("stations_summary_table", default=TransformedStation,partitions=1,changelog_topic=out_topic) 

#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def station(stations):
    
    async for st in stations.group_by(Station.station_name):
   
        if (st.red):
            line = 'red'
        elif (st.blue):
            line = 'blue'
        elif (st.green):
            line = 'green'
        transformed_station = TransformedStation(station_id=st.station_id, station_name=st.station_name, order=st.order, line=line)
        await out_topic.send(key=str(st.station_id), value=transformed_station)


if __name__ == "__main__":
    app.main()
