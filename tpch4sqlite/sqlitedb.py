import csv
import sqlite3
import pandas

columns = {
    "lineitem": "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?)"
}


class SQLiteDB:
    """Class for connections to sqlite3 database
    """
    __connection__ = None
    __cursor__ = None

    def __init__(self, db_name):
        # Exception handling is done by the method using this.
        self.__connection__ = sqlite3.connect(f"{db_name}.db")
        self.__cursor__ = self.__connection__.cursor()

    def close(self):
        if self.__cursor__ is not None:
            self.__cursor__.close()
            self.__cursor__ = None
        if self.__connection__ is not None:
            self.__connection__.close()
            self.__connection__ = None

    def createTables(self, filepath, function=None):
        if function is None:
            function = lambda x: x
        with open(filepath) as f:
            queries = f.readlines()
            query = ""
            has_query = 0
            list_queries = []
            for line in queries:
                if "CREATE TABLE" in line:
                    has_query = 1
                    list_queries.append(query)
                    query = line
                elif has_query:
                    query += line
            list_queries.append(query)

            for q in list_queries:
                qr = function(q)
                self.executeQuery(qr)

    def executeQueryFromFile(self, filepath, function=None):
        if function is None:
            function = lambda x: x
        with open(filepath) as query_file:
            query = query_file.read()
            query = function(query)
            return self.executeQuery(query)

    def executeQuery(self, query):
        if self.__cursor__ is not None:
            self.__cursor__.execute(query)
            return 0
        else:
            print("database has been closed")
            return 1

    def copyFromCSV(self, filepath, separator, table):
        if self.__cursor__ is not None:
            df = pandas.read_csv(filepath, sep=separator, engine="python", header=None)
            df.to_sql(table, self.__connection__, if_exists='append', index=False, chunksize = 10000)
            return 0
        else:
            print("database has been closed")
            return 1

    def commit(self):
        if self.__connection__ is not None:
            self.__connection__.commit()
            return 0
        else:
            print("cursor not initialized")
            return 1
