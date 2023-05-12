import logging
import requests
import time
import datetime
from google.transit import gtfs_realtime_pb2
from google.cloud.sql.connector import Connector
import sqlalchemy


class TransportService:
    def __init__(self) -> None:
        # Initialize a database connection
        # using the Google Cloud SQL Connector
        conn = Connector().connect(
            "handy-limiter-384908:us-central1:lvivtransport",  #peaceful-impact-382015:us-central1:lvivtransport for data before 4 травня 2023 р., 12:38:38
            "pymysql",
            user="root",  #lvivtransport for data before 4 травня 2023 р., 12:38:38
            password="root",
            db="lvivtransportdb"
        )
        # Create a connection pool and connect to the database
        pool = sqlalchemy.create_engine(
            "mysql+pymysql://",
            creator=lambda: conn
        )
        self.db_conn = pool.connect()
        # Create the table to store vehicle data if it does not exist
        self._create_table()

    def _create_table(self):
        # Define the schema for the vehicle data table
        schema = sqlalchemy.text("""CREATE TABLE IF NOT EXISTS vehicle_data (
            id VARCHAR(255) NOT NULL,
            trip_id VARCHAR(255),
            route_id VARCHAR(255),
            vehicle_id VARCHAR(255),
            license_plate VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            bearing FLOAT,
            speed FLOAT,
            timestamp INT)""")
        # Execute the schema creation statement
        self.db_conn.execute(schema)
        self.db_conn.commit()

    def _fetch_vehicles(self):
        # Fetch the vehicle data using the GTFS Realtime Protocol
        logging.info('Fetching vehicles')
        feed = gtfs_realtime_pb2.FeedMessage()
        response = requests.get('http://track.ua-gis.com/gtfs/lviv/vehicle_position')
        feed.ParseFromString(response.content)
        return feed.entity

    def _upload_vehicles_into_database(self, vehicles):
        # Upload the vehicle data into the database
        logging.info('Starting upload for %d vehicles', len(vehicles))
        # Define the SQL statement for inserting the vehicle data
        insert_stmt = sqlalchemy.text(
            """INSERT INTO vehicle_data (id, trip_id, route_id,
                             vehicle_id, license_plate, latitude, longitude,
                             bearing, speed, timestamp) VALUES
                            (:id, :trip_id, :route_id,
                             :vehicle_id, :license_plate, :latitude, :longitude,
                             :bearing, :speed, :timestamp)""",
        )
        # Create a list of parameters for the insert statement
        parameters = []
        for vehicle in vehicles:
            id = vehicle.id
            vehicle = vehicle.vehicle
            parameters.append({
                "id": id,
                "trip_id": vehicle.trip.trip_id,
                "route_id": vehicle.trip.route_id,
                "vehicle_id": vehicle.vehicle.id,
                "license_plate": vehicle.vehicle.license_plate,
                "latitude": vehicle.position.latitude,
                "longitude": vehicle.position.longitude,
                "bearing": vehicle.position.bearing,
                "speed": vehicle.position.speed,
                "timestamp": vehicle.timestamp
            })
        # Execute the insert statement with the parameters
        self.db_conn.execute(insert_stmt, parameters)
        self.db_conn.commit()

    def run(self):
        # Get the current hour
        now = int(datetime.datetime.now().strftime('%H'))
        # Run the fetch and upload loop between 3:00 and 21:00
        while now >= 3 and now <= 21:
            start = time.time()
            self._upload_vehicles_into_database(self._fetch_vehicles())
            diff = time.time() - start
            print(diff)
            key_time = 60
            if key_time - diff > 0:
                sleep = key_time - diff
            else:
                sleep = 0
            time.sleep(sleep)


def main() -> None:
    transport_service = TransportService()
    transport_service.run()


if __name__ == "__main__":
    main()
