import datetime
import logging
import threading
import time
from typing import Optional

import serial


class Report:
    def __init__(self, values: Optional[dict[str, float]] = None, dt: Optional[datetime.datetime] = None):
        self.values = values if values is not None else {}
        self.time = dt if dt is not None else datetime.datetime.utcnow()

    def finished(self):
        for c in ["temp", "winddir", "windspeed", "humid", "dewpoint", "press"]:
            if c not in self.values:
                return False
        return True

    def copy(self):
        return Report(self.values, self.time)


class Lambrecht:
    """Class that operates a Lambrecht meteo weather station."""

    def __init__(
        self,
        port: str = "/dev/ttyUSB0",
        baudrate: int = 4800,
        bytesize: int = 8,
        parity: str = "N",
        stopbits: int = 1,
        rtscts: bool = False,
        timeout: int = 10,
        *args,
        **kwargs
    ):
        """

        Args:
            port: Serial port to use.
            baudrate: Baud rate.
            bytesize: Size of bytes.
            parity: Parity.
            stopbits: Stop bits.
            rtscts: RTSCTS.
            *args:
            **kwargs:
        """

        # serial connection
        self._conn = None
        self._port = port
        self._baudrate = baudrate
        self._bytesize = bytesize
        self._parity = parity
        self._stopbits = stopbits
        self._rtscts = rtscts
        self._serial_timeout = timeout

        # poll thread
        self._closing = None
        self._thread = None
        self._thread_sleep = 1
        self._max_thread_sleep = 900

        # init
        self._serial_errors = 0
        self._sleep_time = self._thread_sleep
        self._raw_data = b""

        # callback function
        self._callback = None

        # init values
        self._report = Report()

    def start_polling(self, callback):
        """Start polling the Lambrecht meteo weather station.

        Args:
            callback: Callback function to be called with new data.
        """

        # set callback
        self._callback = callback

        # start thread
        self._closing = threading.Event()
        self._thread = threading.Thread(target=self._poll_thread)
        self._thread.start()

    def stop_polling(self):
        """Stop polling of Lambrecht meteo weather station."""

        # close and wait for thread
        self._closing.set()
        self._thread.join()

    def _poll_thread(self):
        """Thread to poll and respond to the serial output of the Lambrecht meteo weather station.

        The thread places output into a circular list of parsed messages stored as
        dictionaries containing the response itself, the datetime of the response
        and the type of response.  The other methods normally only access the most current report.
        """

        # init
        self._serial_errors = 0
        self._sleep_time = self._thread_sleep
        self._raw_data = b""

        # loop until closing
        while not self._closing.is_set():
            try:
                self._poll()
            except:
                # sleep a little and continue
                logging.exception("Somethingw went wrong")
                time.sleep(10)

        # close connection
        self._conn.close()

    def _poll(self):
        # get serial connection
        if self._conn is None:
            logging.info("connecting to Boltwood II sensor")
            try:
                # connect
                self._connect_serial()

                # reset sleep time
                self._serial_errors = 0
                self._sleep_time = self._thread_sleep

            except serial.SerialException as e:
                # if no connection, log less often
                self._serial_errors += 1
                if self._serial_errors % 10 == 0:
                    if self._sleep_time < self._max_thread_sleep:
                        self._sleep_time *= 2
                    else:
                        self._sleep_time = self._thread_sleep

                # do logging
                logging.critical(
                    "%d failed connections to Lambrecht: %s, sleep %d",
                    self._serial_errors,
                    str(e),
                    self._sleep_time,
                )
                self._closing.wait(self._sleep_time)

        # actually read next line and process it
        if self._conn is not None:
            try:
                self._raw_data = self._read_data(self._raw_data)
            except:
                self._closing.wait(self._sleep_time)
                return

    def _read_data(self, raw_data: bytes):
        # read data
        self._raw_data += self._conn.read()

        # extract messages
        msgs, self._raw_data = self._extract_messages(self._raw_data)

        # analyse it and return remaining data
        for msg in msgs:
            self._analyse_message(msg)
        return self._raw_data

    def _extract_messages(self, raw_data) -> (list, bytearray):
        """Extract all complete messages from the raw data from the Boltwood.

        Args:
            self._raw_data: bytearray from Boltwood (via serial.readline())

        Returns:
            List of messages and remaining raw data.

        Normally, there should just be a single message per readline, but....
        """

        # nothing?
        if not self._raw_data:
            return [], b""

        # find complete messages
        msgs = []
        while b"\n" in self._raw_data:
            # get message
            pos = self._raw_data.index(b"\n")
            msg = self._raw_data[: pos + 1]

            # store it
            msgs.append(msg)

            # remove from self._raw_data
            self._raw_data = self._raw_data[pos + 1 :]

        # return new self._raw_data and messages
        return msgs, self._raw_data

    def _analyse_message(self, raw_data):
        """Analyse raw message.

        Args:
            self._raw_data: Raw data.

        Returns:

        """

        # no data?
        if len(self._raw_data) == 0 or self._raw_data == b"\n":
            return

        # to string
        line = self._raw_data.decode()

        # split line and check first token
        s = line.split(",")
        if s[0] == "$WIMTA":
            self._report.values["temp"] = float(s[1])
        elif s[0] == "$WIMWV":
            self._report.values["winddir"] = float(s[1])
            self._report.values["windspeed"] = float(s[3])
        elif s[0] == "$WIMHU":
            self._report.values["humid"] = float(s[1])
            self._report.values["dewpoint"] = float(s[3])
        elif s[0] == "$WIMMB":
            self._report.values["press"] = float(s[3])

        # finished?
        if self._report.finished():
            self._report.time = datetime.datetime.utcnow()
            self._callback(self._report)
            self._report = Report()

    def _connect_serial(self):
        """Open/reset serial connection to sensor."""

        # close first?
        if self._conn is not None and self._conn.is_open:
            self._conn.close()

        # create serial object
        self._conn = serial.Serial(
            self._port,
            self._baudrate,
            bytesize=self._bytesize,
            parity=self._parity,
            stopbits=self._stopbits,
            timeout=self._serial_timeout,
            rtscts=self._rtscts,
        )

        # open it
        if not self._conn.is_open:
            self._conn.open()


__all__ = ["Lambrecht"]
