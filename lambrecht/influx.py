import datetime
import queue
import threading
import time
from typing import Optional

import urllib3
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

from boltwood.report import Report, SensorsReport


FIELDS = {
    "ambientTemperature": "temp",
    "relativeHumidityPercentage": "humid",
    "windSpeed": "windspeed",
    "skyMinusAmbientTemperature": "skytemp",
    "rainSensor": "rain",
}


class Influx:
    def __init__(
        self,
        url: Optional[str] = None,
        token: Optional[str] = None,
        org: Optional[str] = None,
        bucket: Optional[str] = None,
    ):
        # init db connection
        self._client: Optional[InfluxDBClient] = None
        self._bucket: Optional[str] = None
        if url is not None and token is not None and org is not None and bucket is not None:
            self._client = InfluxDBClient(url=url, token=token, org=org)
            self._bucket = bucket

        # init queue
        self._queue = queue.Queue()

        # thread
        self._closing = threading.Event()
        self._thread = threading.Thread(target=self._send_measurements)

    def start(self):
        """Start thread."""
        self._thread.start()

    def stop(self):
        """End thread."""
        self._closing.set()
        self._thread.join()

    def __call__(self, dt: datetime.datetime, values: dict[str, float]):
        """Put a new measurement in the send queue."""
        if self._client is not None:
            self._queue.put((dt, values))

    def _send_measurements(self):
        """Run until closing to send reports."""

        # no client?
        if self._client is None:
            return

        # get API
        write_api = self._client.write_api(SYNCHRONOUS)

        # run (almost) forever
        while not self._closing.is_set():
            # get next values to send
            (dt, values) = self._queue.get()

            # send it
            try:
                write_api.write(
                    bucket=self._bucket,
                    record={
                        "measurement": "lambrecht",
                        "fields": values,
                        "time": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    },
                )
            except urllib3.exceptions.NewConnectionError:
                # put message back and wait a little
                self._queue.put((dt, values))
                time.sleep(10)
