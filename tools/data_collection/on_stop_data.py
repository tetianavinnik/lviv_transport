from math import atan2, radians, cos, sin, sqrt
from google.cloud.sql.connector import Connector
import sqlalchemy
import datetime
import pytz


class TransportService:
    def __init__(self) -> None:
        # Initialize a database connection
        # using the Google Cloud SQL Connector
        conn = Connector().connect(
            "handy-limiter-384908:us-central1:lvivtransport",  #peaceful-impact-382015:us-central1:lvivtransport for data before 4 травня 2023 р., 12:38:38
            "pymysql",
            user="root",
            password="root",  #lvivtransport for data before 4 травня 2023 р., 12:38:38
            db="lvivtransportdb"
        )
        # Create a connection pool and connect to the database
        pool = sqlalchemy.create_engine(
            "mysql+pymysql://",
            creator=lambda: conn
        )
        self.db_conn = pool.connect()
        # Create the table to store on-stop data if it does not exist
        self._create_table()

    def _create_table(self):
        # Define the schema for the on-stop data table
        schema = sqlalchemy.text("""CREATE TABLE IF NOT EXISTS on_stop_data (
            id VARCHAR(255) NOT NULL,
            trip_id VARCHAR(255),
            route_id VARCHAR(255),
            vehicle_id VARCHAR(255),
            license_plate VARCHAR(255),
            latitude FLOAT,
            longitude FLOAT,
            bearing FLOAT,
            speed FLOAT,
            timestamp INT,
            stop_id VARCHAR(255),
            stop_code VARCHAR(255),
            stop_name VARCHAR(255),
            stop_desc VARCHAR(255),
            stop_lat FLOAT,
            stop_lon FLOAT,
            bearing1 INT,
            bearing2 INT)""")
        # Execute the schema creation statement
        self.db_conn.execute(schema)
        self.db_conn.commit()

    def _upload_vehicles_into_database(self):
        # Define the SQL statement for inserting the on-stop data
        insert_stmt = sqlalchemy.text(
            """INSERT INTO on_stop_data (id, trip_id, route_id,
                             vehicle_id, license_plate, latitude, longitude,
                             bearing, speed, timestamp, stop_id, stop_code,
                             stop_name, stop_desc, stop_lat, stop_lon,
                             bearing1, bearing2) VALUES
                            (:id, :trip_id, :route_id,
                             :vehicle_id, :license_plate, :latitude, :longitude,
                             :bearing, :speed, :timestamp, :stop_id, :stop_code,
                             :stop_name, :stop_desc, :stop_lat, :stop_lon, :bearing1,
                             :bearing2)""",
        )

        self._analyze(insert_stmt)
        self.db_conn.commit()

    def _haversine(self, lat1, lon1, lat2, lon2):
        # Calculate the distance between two points
        # using the Haversine formula
        R = 6371000  # radius of Earth in meters
        phi1 = radians(lat1)
        phi2 = radians(lat2)
        delta_phi = radians(lat2 - lat1)
        delta_lambda = radians(lon2 - lon1)
        a = sin(delta_phi/2)**2 + cos(phi1)*cos(phi2)*sin(delta_lambda/2)**2
        c = 2*atan2(sqrt(a), sqrt(1 - a))
        distance = R*c
        return distance

    def _analyze(self, insert_stmt):
        # Analyze the vehicle data and identify vehicles
        # that are in 100 meter range from specific stops
        kyiv_tz = pytz.timezone('Europe/Kiev')

        now_kyiv = datetime.datetime.now(kyiv_tz)

        start_time = now_kyiv.replace(hour=6, minute=0, second=0, microsecond=0).timestamp()
        end_time = now_kyiv.replace(hour=22, minute=30, second=0, microsecond=0).timestamp()

        query = sqlalchemy.text(f"SELECT * FROM vehicle_data WHERE timestamp >= {start_time} AND timestamp <= {end_time}")
        result = self.db_conn.execute(query)

        with open(r"/home/tetiana/Transport/TimerTrigger1/stops_10.csv", "r", encoding="utf-8") as f:
            stop_lst = []
            for stop in f:
                if stop[0] != 's':
                        stop_lst.append(stop)
            parameters = []

            for row in result:
                id, trip_id, route_id, vehicle_id, license_plate, latitude, longitude, bearing, speed, timestamp = row
                #routes 3a, 16, 47, 46, 52, 22, 5а, 39, 34, 61
                if route_id in {"94", "112", "102", "1001", "117", "2299", "2355", "992", "146", "1884"}:
                    for stop in stop_lst:
                        stop = stop.strip().split(',')
                        stop_id = 1 # stop[0]
                        stop_code = "" #stop[1]
                        stop_name = stop[0]
                        stop_desc = "" #stop[3]s
                        stop_lat = float(stop[-4])
                        stop_lon = float(stop[-3])
                        stop_b1 = int(stop[-2])
                        stop_b2 = int(stop[-1])
                        distance = self._haversine(stop_lat, stop_lon, latitude, longitude)
                        if distance <= 100:
                            check = False
                            if stop_b1 > stop_b2:
                                if (bearing >= stop_b1 and bearing <= 360) or (bearing >= 0 and bearing <= stop_b2):
                                    check = True
                            else:
                                if bearing >= stop_b1 and bearing <= stop_b2:
                                    check = True
                            if check:
                                parameters.append({
                                    "id": id,
                                    "trip_id": trip_id,
                                    "route_id": route_id,
                                    "vehicle_id": vehicle_id,
                                    "license_plate": license_plate,
                                    "latitude": latitude,
                                    "longitude": longitude,
                                    "bearing": bearing,
                                    "speed": speed,
                                    "timestamp": timestamp,
                                    "stop_id": stop_id,
                                    "stop_code": stop_code,
                                    "stop_name": stop_name,
                                    "stop_desc": stop_desc,
                                    "stop_lat": stop_lat,
                                    "stop_lon": stop_lon,
                                    "bearing1": stop_b1,
                                    "bearing2": stop_b2
                                    })
            self.db_conn.execute(insert_stmt, parameters)
            self.db_conn.commit()

    def run(self):
        self._upload_vehicles_into_database()


def main() -> None:
    transport_service = TransportService()
    transport_service.run()


if __name__ == "__main__":
    main()
