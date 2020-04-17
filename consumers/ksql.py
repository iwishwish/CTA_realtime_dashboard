"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

#
# Done: Complete the following KSQL statements.
# Done: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!
# Done: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`
#       Make sure to set the value format to JSON
KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR 
) WITH (
    KAFKA_TOPIC='org.chicago.cta.stations.turnstile',
    VALUE_FORMAT='avro',
    KEY='station_id'
);
CREATE TABLE TURNSTILE_SUMMARY 
WITH (VALUE_FORMAT='json')
AS SELECT COUNT(station_id) AS count, station_id
FROM turnstile 
GROUP By station_id ;

"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    try:
        resp.raise_for_status()
    except:
        logging.debug(f"Ksql failed:{json.dumps(resp.json(), indent=2)}")
        print(json.dumps(resp.json(), indent=2))


if __name__ == "__main__":
    execute_statement()
