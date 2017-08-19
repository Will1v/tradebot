from crate import client

class CrateDbInterface(object):
    """
    DB Interface to use with CrateDB
    """

    init_tables = {
        'market_data_histo': "CREATE TABLE market_data_histo (" \
                                       "mdh_timestamp timestamp, " \
                                       "ccy_id string, " \
                                       "bid_qty integer, " \
                                       "bid float, " \
                                       "ask float, " \
                                       "ask_qty integer, " \
                                       "primary key (mdh_timestamp, ccy_id))",
        'order_book_histo': "CREATE TABLE order_book_histo ("
                            "obh_timestamp timestamp, "
                            "ccy_id string, "
                            "order_book string, "
                            "primary key (obh_timestamp, ccy_id))"
    }

    def __init__(self, db_logger):
        self.logger = db_logger
        try:
            self.connection = client.connect("localhost:4200")
        except:
            self.logger.warning("Couldn't connect to DB!")
        self.cursor = self.connection.cursor()

    def format_query(self, query):
        return """""""{}""""""".format(query)

    def drop_and_create_db(self):

        self.logger.debug("Initiating init_tables with tables: {}".format(self.init_tables.keys()))

        for t in self.init_tables:
            try:
                self.logger.debug("Trying to drop table: {}".format(t))
                drop_query = "DROP TABLE {}".format(t)
                self.cursor.execute(self.format_query(drop_query))
                self.logger.info("Table {} dropped".format(t))
            except:
                self.logger.warning("Couldn't drop table {}, perhaps the table doesn't exist?".format(t))

            self.cursor.execute(self.format_query(self.init_tables[t]))
            self.logger.info("Table {} created".format(t))

    def run_query(self, query):
        self.logger.debug("Running command:\n{}\n".format(self.format_query(query)))
        self.cursor.execute(self.format_query(query))
