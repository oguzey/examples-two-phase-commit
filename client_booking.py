import psycopg2
import collections
import logging as logger

logger.basicConfig(format='%(levelname)s:  %(message)s', level=logger.DEBUG)

Settings = collections.namedtuple('Settings', ['dbname_flies',
                                               'dbname_hotels',
                                               'tbname_flies',
                                               'tbname_hotels',
                                               'user',
                                               'password',
                                               'host'])

settings = Settings('data_fly_booking', 'data_hotel_booking',
                    'fly_booking', 'hotel_booking',
                    'test_user', 'local', 'localhost')


class DBBookingWorker(object):

    def __new__(cls, *args, **kwargs):
        configs = kwargs.get('settings')
        if configs is None or not isinstance(configs, Settings):
            logger.critical("No settings were provided or they were invalid.")
            return None

        conn_db_flies = psycopg2.connect(dbname=configs.dbname_flies,
                                         user=configs.user,
                                         password=configs.password,
                                         host=configs.host)
        conn_db_hotels = psycopg2.connect(dbname=configs.dbname_hotels,
                                          user=configs.user,
                                          password=configs.password,
                                          host=configs.host)
        if not conn_db_flies or not conn_db_hotels:
            logger.critical("Could not connect to databases '{}', '{}'"
                            .format(configs.dbname_flies,
                                    configs.dbname_hotels))
            return None
        new_instance = object.__new__(cls, *args, **kwargs)
        setattr(new_instance, 'dbconn_fly_booking', conn_db_flies)
        setattr(new_instance, 'dbconn_hotel_booking', conn_db_hotels)
        return new_instance

    def __del__(self):
        self.dbconn_fly_booking.close()
        self.dbconn_hotel_booking.close()

    def __init__(self, settings=settings):
        super(DBBookingWorker, self).__init__()
        self.settings = settings
        self.dbconn_fly_booking = self.dbconn_fly_booking
        self.dbconn_hotel_booking = self.dbconn_hotel_booking

    def show_all_booking_flies(self):
        # cur.fetchone()
        cursor = self.dbconn_fly_booking.cursor()
        cursor.execute('SELECT * FROM {};'.format(self.settings.tbname_flies))
        logger.info("Data from table '{}'".format(self.settings.tbname_flies))
        for row in cursor:
            logger.info("{}".format(row))
        cursor.close()

    def show_all_booking_hotels(self):
        cursor = self.dbconn_hotel_booking.cursor()
        cursor.execute("SELECT * FROM {};".format(self.settings.tbname_hotels))
        logger.info("Data from table '{}'".format(self.settings.tbname_hotels))
        for row in cursor:
            logger.info("{}".format(row))
        cursor.close()

    def make_two_phase_transaction(self, func_transactions_cb):
        assert callable(func_transactions_cb)

        ready = psycopg2.extensions.STATUS_READY
        if self.dbconn_fly_booking.status != ready or self.dbconn_hotel_booking.status != ready:
            logger.error("Databases are not ready for two phase transaction.")
            return
        xid = psycopg2.extensions.Xid.from_string("random_string")
        self.dbconn_fly_booking.tpc_begin(xid)

        # Do actions with both connections
        func_transactions_cb()

        try:
            self.dbconn_fly_booking.tpc_prepare()
            self.dbconn_hotel_booking.commit()
        except (psycopg2.DatabaseError, psycopg2.ProgrammingError) as er:
            logger.warn("Rollback. Fail occurred during transaction.")
            logger.warn("Error was '{}'".format(er.message))
            self.dbconn_fly_booking.tpc_rollback()
            self.dbconn_hotel_booking.rollback()
        else:
            logger.info("Commit. Done successful transaction.")
            self.dbconn_fly_booking.tpc_commit()


logger.info("Start program ...")


worker = DBBookingWorker(settings=settings)


def show_all_data():
    worker.show_all_booking_flies()
    worker.show_all_booking_hotels()

worker.make_two_phase_transaction(show_all_data)
logger.info("End program.")
