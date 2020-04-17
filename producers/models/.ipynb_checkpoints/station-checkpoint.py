"""Methods pertaining to loading and configuring CTA "L" station data."""
import logging, json
from pathlib import Path

from confluent_kafka import avro

from models import Turnstile
from models.producer import Producer

from avro.schema import Field


# Fixes an issue with python3-avro:
# https://github.com/confluentinc/confluent-kafka-python/issues/610
Field.to_json_old = Field.to_json
def to_json(self, names=None):
    to_dump = self.to_json_old(names)
    type_name = type(to_dump["type"]).__name__
    if type_name == "mappingproxy":
        to_dump["type"] = to_dump["type"].copy()
    return to_dump
Field.to_json = to_json


logger = logging.getLogger(__name__)


class Station(Producer):
    """Defines a single station"""

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")

    #
    # Done: Define this value schema in `schemas/station_value.json, then uncomment the below
    #
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")
   

    def __init__(self, station_id, name, color, direction_a=None, direction_b=None):
        self.name = name
        station_name = (
            self.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        #
        #
        # Done: Complete the below by deciding on a topic name, number of partitions, and number of
        # replicas
        #
        #
        topic_name = f"org.chicago.cta.station.arrivals" # TODO: Come up with a better topic name
        super().__init__(
            topic_name,
            key_schema=Station.key_schema,
            value_schema=Station.value_schema, # TODO: Uncomment once schema is defined
            num_partitions=3,
            num_replicas=3,
        )

        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None
        self.turnstile = Turnstile(self)


    def run(self, train, direction, prev_station_id, prev_direction):
        """Simulates train arrivals at this station"""
        #
        #
        # Done: Complete this function by producing an arrival message to Kafka
        #
        #
        #logger.info("arrival kafka integration incomplete - skipping")
        arrival = {
            "station_id": self.station_id,
            "train_id": train.train_id,
            "direction": direction,
            "prev_station_id":prev_station_id,
            "prev_direction":prev_direction,
            "train_status": train.status.name,
            "line": self.color.name
        }
        self.producer.produce(
            topic=self.topic_name,
            key={"timestamp": self.time_millis()},
            key_schema=self.key_schema,
            value_schema=self.value_schema,
            value=arrival
        )
        logger.info(f"send arrivals info to kafka: {arrival.items()}")

    

    def __str__(self):
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self):
        return str(self)

    def arrive_a(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(self, train, prev_station_id, prev_direction):
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.turnstile.close()
        super().close()