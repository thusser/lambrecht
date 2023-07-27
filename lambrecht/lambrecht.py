import datetime
import threading


class Lambrecht:
    """Class that operates a Lambrecht meteo weather station."""

    def __init__(self, dev_file: str, *args, **kwargs):
        """

        Args:
            dev_file: Device file for input.
            *args:
            **kwargs:
        """

        # store
        self._dev_file = dev_file

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

        # init values
        values = {"temp": 0.0, "windspeed": 0.0, "winddir": 0.0, "humid": 0.0, "dewpoint": 0.0, "press": 0.0}

        # open file
        with open(self._dev_file, "r") as dev:
            # loop until closing
            while not self._closing.is_set():
                # read next line, and skip lines not starting with $
                line = dev.readline().strip()
                if not line.startswith("$"):
                    continue

                # split line and check first token
                s = line.split()
                if s[0] == "$WIMTA":
                    values["temp"] = float(s[1])
                elif s[0] == "$WIMWV":
                    values["winddir"] = float(s[1])
                    values["windspeed"] = float(s[3])
                elif s[0] == "$WIMHU":
                    values["humid"] = float(s[1])
                    values["dewpoint"] = float(s[3])
                elif s[0] == "$WIMMB":
                    values["press"] = float(s[1])
                    self._callback(datetime.datetime.utcnow(), values)


__all__ = ["Lambrecht"]
