import time
import logging
import datetime

import psycopg2

log = logging.getLogger(__name__)


class ActivityWatcher:
    availability_wait_interval = datetime.timedelta(minutes=10)

    def __init__(self, db_host, max_inactive_dt, callback):
        self.db_host = db_host
        self.max_inactive_dt = max_inactive_dt
        self.callback = callback
        self._cancel = False

    def start(self):
        self._cancel = False

        try:
            self._run()
        finally:
            if not self._cancel:
                self.callback()

    def _run(self):
        log.info('Watching postgres server activity...')
        last_activity = datetime.datetime.now()

        while datetime.datetime.now() - last_activity < self.max_inactive_dt:
            time.sleep(60)
            if self._cancel:
                return
            if self.has_activity():
                log.info('There is some activity')
                last_activity = datetime.datetime.now()
            else:
                log.info('There is no current activity')

        log.info('There was no activity since %s, finishing', last_activity)

    def wait_availability(self):
        log.info('Waiting for postgres server...')
        start = datetime.datetime.now()

        while datetime.datetime.now() - start < self.availability_wait_interval:
            try:
                conn = psycopg2.connect(f'dbname=postgres user=postgres host={self.db_host}')
            except Exception:
                log.info('Server not available', exc_info=True)
                time.sleep(15)
            else:
                log.info('Server is available')
                conn.close()
                break

        raise Exception(f'Server not available since {start}')

    def has_activity(self):
        conn = psycopg2.connect(f'dbname=postgres user=postgres host={self.db_host}')
        cur = conn.cursor()
        cur.execute(
            """SELECT pid
            FROM pg_stat_activity
            WHERE backend_type='client backend' AND pid<>pg_backend_pid()
            LIMIT 1"""
        )
        ret = cur.fetchone()

        cur.close()
        conn.close()

        return ret is not None

    def cancel(self):
        self._cancel = True
