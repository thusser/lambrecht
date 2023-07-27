import argparse
import datetime
import json
import os
import tornado.ioloop
import tornado.web
import tornado.httpserver
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging

from lambrecht.influx import Influx
from .lambrecht import Lambrecht


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        app: Application = self.application
        self.render(os.path.join(os.path.dirname(__file__), 'template.html'),
                    current=app.current, history=app.history)


class JsonHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header('Content-Type', 'application/json')

    def get(self, which):
        """JSON output of data.

        Args:
            which: "current" or "average".

        Returns:
            JSON output.
        """

        # get record
        if which == 'current':
            record = self.application.current
        elif which == 'average':
            record = self.application.average
        else:
            raise tornado.web.HTTPError(404)

        # get data
        data = {'time': record.time.strftime('%Y-%m-%d %H:%M:%S')}
        for c in ['ambientTemperature', 'relativeHumidityPercentage', 'windSpeed',
                  'skyMinusAmbientTemperature', 'rainSensor']:
            data[c] = record.data[c] if c in record.data else 'N/A'

        # send to client
        self.write(json.dumps(data))


class Application(tornado.web.Application):
    def __init__(self, log_file: str = None, *args, **kwargs):
        # static path
        static_path = os.path.join(os.path.dirname(__file__), 'static_html/')

        # init tornado
        tornado.web.Application.__init__(self, [
            (r'/', MainHandler),
            (r'/(.*).json', JsonHandler),
            (r'/static/(.*)', tornado.web.StaticFileHandler, {'path': static_path})
        ])

        # init other stuff
        self.current = None
        self.reports = []
        self.history = []
        self.log_file = log_file
        self.thresholds = None
        self.wetness = None
        self.wetness_calib = None
        self.thermo_calib = None

        # load history
        self._load_history()

    @property
    def average(self):
        return self.history[0] if len(self.history) > 0 else Report()

    def callback(self, report):
        pass

    def _load_history(self):
        """Load history from log file"""

        # no logfile?
        if self.log_file is None or not os.path.exists(self.log_file):
            return

        # open file
        with open(self.log_file, 'r') as csv:
            # check header
            if csv.readline() != 'time,T_ambient,humidity,windspeed,dT_sky,raining\n':
                logging.error('Invalid log file format.')
                return

            # read lines
            for line in csv:
                # split and check
                s = line.split(',')
                if len(s) != 6:
                    logging.error('Invalid log file format.')
                    continue

                # create report and fill it
                report = AverageSensorsReport([])
                report.time = datetime.datetime.strptime(s[0], '%Y-%m-%dT%H:%M:%S')
                report.data = {
                    'ambientTemperature': float(s[1]),
                    'relativeHumidityPercentage': float(s[2]),
                    'windSpeed': float(s[3]),
                    'skyMinusAmbientTemperature': float(s[4]),
                    'rainSensor': s[5] == 'True',
                }
                self.history.append(report)

        # crop
        self._crop_history()

    def _crop_history(self):
        # sort history
        self.history = sorted(self.history, key=lambda h: h.time, reverse=True)

        # crop to 10 entries
        if len(self.history) > 10:
            self.history = self.history[:10]

    def sched_callback(self):
        # average reports
        average = AverageSensorsReport(self.reports)
        self.history.append(average)
        self._crop_history()

        # write to log file?
        if self.log_file is not None:
            # does it exist?
            if not os.path.exists(self.log_file):
                # write header
                with open(self.log_file, 'w') as csv:
                    csv.write('time,T_ambient,humidity,windspeed,dT_sky,raining\n')

            # write line
            with open(self.log_file, 'a') as csv:
                fmt = '{time},' \
                      '{ambientTemperature:.2f},' \
                      '{relativeHumidityPercentage:.2f},' \
                      '{windSpeed:.2f},' \
                      '{skyMinusAmbientTemperature:.2f},' \
                      '{rainSensor}\n'
                csv.write(fmt.format(time=average.time.strftime('%Y-%m-%dT%H:%M:%S'), **average.data))

        # reset reports
        self.reports = []


def main():
    # parser
    parser = argparse.ArgumentParser("Lambrecht meteo data logger")
    parser.add_argument("--log-file", type=str, help="Log file for average values")
    parser.add_argument("--influx", type=str, help="Four strings containing URL, token, org, and bucket", nargs=4)
    args = parser.parse_args()

    # create Lambrecht object
    lambrecht = Lambrecht(**vars(args))

    # init app
    application = Application(**vars(args))

    # influx
    influx = Influx(*args.influx)
    influx.start()

    # callback method
    def callback(report: Report):
        # forward to application and influx
        application.callback(report)
        influx(report)

    # start polling
    lambrecht.start_polling(callback)

    # init tornado web server
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)

    # scheduler
    sched = BackgroundScheduler()
    trigger = CronTrigger(minute="*/5")
    sched.add_job(application.sched_callback, trigger)
    sched.start()

    # start loop
    try:
        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt:
        pass

    # stop polling
    influx.stop()
    lambrecht.stop_polling()
    sched.shutdown()


if __name__ == '__main__':
    main()
