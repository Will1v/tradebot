from crate import client

class CrateDbInterface(object):
    """
    DB Interface to use with CrateDB
    """

    init_tables = {
        'market_data_histo': "CREATE TABLE market_data_histo (" \
                                       "timestamp timestamp, " \
                                       "ccy_id string, " \
                                       "bid_qty float, " \
                                       "bid float, " \
                                       "ask float, " \
                                       "ask_qty float, " \
                                       "primary key (timestamp, ccy_id))",
        'raw_market_data_histo': "CREATE TABLE raw_market_data_histo (" \
                             "timestamp timestamp, " \
                             "ccy_id string, " \
                             "bid_qty float, " \
                             "bid float, " \
                             "ask float, " \
                             "ask_qty float, " \
                             "primary key (timestamp, ccy_id))",
        'order_book_histo': "CREATE TABLE order_book_histo ("
                            "timestamp timestamp, "
                            "ccy_id string, "
                            "order_book string, "
                            "primary key (timestamp, ccy_id))"
    }

    def __init__(self, db_logger):
#       type: (Logger) -> object

        self.logger = db_logger
        try:
            self.connection = client.connect("localhost:4200")
        except:
            self.logger.warning("Couldn't connect to DB!")
        self.cursor = self.connection.cursor()

        #self.queries_stack =

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

    def stack_query(self, query):
        pass


    def run_query(self, query):
        self.logger.debug("Running command:\n{}\n".format(self.format_query(query)))
        try:
            self.cursor.execute(self.format_query(query))
        except:
            self.logger.warning("Query {} failed!".format(query))

    def insert_query(self, table, data_dict):
        # type: (str, dict) -> str
        self.logger.debug("Generating INSERT query for table = {} and data = \n{}".format(table, data_dict))
        query = "INSERT INTO {} ".format(table)
        keys = ""
        values = ""
        for k, v in data_dict.iteritems():
            keys += "{}, ".format(str(k).replace("\'", "\'\'"))
            values += "\'{}\', ".format(str(v).replace("\'", "\'\'"))
        query += "({}) VALUES ({})".format(keys[:-2], values[:-2])  #using [:-2] to remove the extra ", " of keys and values strings.
        self.logger.debug("Returning query: \n{}".format(query))
        return query
