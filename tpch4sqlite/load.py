import os
from .sqlitedb import SQLiteDB


def clean_database(db_name, tables):
    try:
        conn = SQLiteDB(db_name)
        try:
            for table in tables:
                conn.executeQuery("DROP TABLE IF EXISTS %s " % table)
        except Exception as e:
            print("unable to remove existing tables. %s" % e)
            return 1
        print("dropped existing tables")
        conn.commit()
        conn.close()
        return 0
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1


def create_schema(query_root, db_name, prep_query_dir):
    try:
        conn = SQLiteDB(db_name)
        try:
            queries = os.path.join(query_root, prep_query_dir, "create_tbl.sql")
            conn.createTables(queries)
        except Exception as e:
            print("unable to run create tables. %s" % e)
            return 1
        conn.commit()
        conn.close()
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1


def load_tables(data_dir, db_name, tables, load_dir):
    try:
        conn = SQLiteDB(db_name)
        try:
            for table in tables[0:2]:
                filepath = os.path.join(data_dir, load_dir, table.lower() + ".tbl.csv")
                print(f"filepath={filepath}")
                conn.copyFromCSV(filepath, separator="|", table=table.lower())
            conn.commit()
        except Exception as e:
            print("unable to run load tables. %s" %e)
            return 1
        conn.close()
        return 0
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1


def index_tables(query_root, db_name, prep_query_dir):
    try:
        conn = SQLiteDB(db_name)
        try:
            conn.executeQueryFromFile(os.path.join(query_root, prep_query_dir, "create_idx.sql"))
            conn.commit()
        except Exception as e:
            print("unable to run index tables. %s" % e)
            return 1
        conn.close()
        return 0
    except Exception as e:
        print("unable to connect to the database. %s" % e)
        return 1
