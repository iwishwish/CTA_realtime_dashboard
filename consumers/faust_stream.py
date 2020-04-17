"""Defines trends calculations for stations"""
import logging

import faust
import topic_check


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
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
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Done: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app = faust.App("faust_stream", broker="kafka://localhost:9092", store="memory://")
# Done: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic = app.topic("org.chicago.cta.connect-stations", value_type=Station, partitions=1)
# Done: Define the output Kafka Topic
out_topic_name = "org.chicago.cta.stations.table.faust"
topic_check.build_topic(out_topic_name, num_partitions=1)
out_topic = app.topic(out_topic_name, partitions=1)
# TODO: Define a Faust Table
#table = app.Table(
#    'line_stations_summary',
#    default=int,
#    partitions=1
#)


#
#
# Done: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
def transformed_station(station):
    line = None
    if station.red == True:
        line = "red"
    elif station.blue == True:
        line = "blue"
    elif station.green == True:
        line = "green"
        
    transformed_station = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line)
    return transformed_station

@app.agent(topic, offset='earliest')
async def tranform(stations):
    stations.add_processor(transformed_station)
    async for station in stations:
        await out_topic.send(value = station)

if __name__ == "__main__":
    app.main()
