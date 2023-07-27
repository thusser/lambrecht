import logging
import time
import serial
import threading

from .report import Report, SensorsReport


class Lambrecht:
    """Class that operates a Lambrecht meteo weather station."""

    def __init__(self, *args, **kwargs):
        """

        Args:
            *args:
            **kwargs:
        """

        # poll thread
        self._closing = None
        self._thread = None
        self._thread_sleep = 1
        self._max_thread_sleep = 900

        # callback function
        self._callback = None

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
        serial_errors = 0
        sleep_time = self._thread_sleep
        last_report = None
        raw_data = b''

        # loop until closing
        while not self._closing.is_set():
            # actually read next line and process it
            raw_data += self._conn.read()

            # extract messages
            msgs, raw_data = self._extract_messages(raw_data)

            # analyse it
            for msg in msgs:
                self._analyse_message(msg)
                last_report = time.time()


__all__ = ['Lambrecht']
