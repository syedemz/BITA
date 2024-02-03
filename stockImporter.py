import psycopg2
from psycopg2 import sql
from importhelper import check_previous_import, bulk_import, set_import_date


class stockImporter:
    def __init__(self, dbname, user, password, schema, tablename, host='localhost', port=5432):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.schema = schema
        self.tablename = tablename

    def connect(self):
        try:
            # Connect to the PostgreSQL server
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            print("connected successfully")

        except Exception as e:
            print(f"Unable to connect to the database. Error: {e}")


    # get the schema from the helper , dont hardcode
    def create_table(self):
        try:
            create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {self.schema}.{self.tablename} (
                    PointOfSale VARCHAR(255),
                    Product VARCHAR(255),
                    Date VARCHAR(255),
                    Stock VARCHAR(255)
                );
            """
            # Execute the query
            self.cursor.execute(create_table_query)

            # Commit the changes
            self.conn.commit()

            print(f"Table {self.schema}.{self.tablename} created successfully")

        except Exception as e:
            print(f"Table {self.schema}.{self.tablename} could not created")
            print(f"Error: {e}")



    def begin_import(self):

        if(check_previous_import()):
            
            try:
                print("removing previously imported data from table")
                self.cursor.execute(f"DELETE FROM {self.schema}.{self.tablename}")
                print("old data removed from table")
                print("starting new data import ........")
                bulk_import(self.host, self.port, self.dbname, self.user, self.password, self.schema, self.tablename, 'alchemy')
                print("import in progress ......... ")
                set_import_date()


            except Exception as e:
                print(f"Error removing old data: {e}")
                return False
           
        else:
            print("starting new data import ........")
            bulk_import(self.host, self.port, self.dbname, self.user, self.password, self.schema, self.tablename, 'alchemy')
            print("import in progress ......... ")
            set_import_date()
      

    def check_table_exists(self, table_name):
        try:
            # Check if the table exists
            self.cursor.execute(f"SELECT * FROM information_schema.tables WHERE table_schema = '{self.schema}' AND table_name = '{self.tablename}'")
   
            result = self.cursor.fetchone()

            if result:
                print(f"Table {table_name} exists.")
                return True
            else:
                print(f"Table {table_name} does not exist.")
                return False
        except Exception as e:
            print(f"Error checking table existence: {e}")
            return False

    def close_connection(self):
        try:
            # Close the connection
            if self.conn is not None:
                self.conn.close()

        except Exception as e:
            print(f"Error closing connection: {e}")


if __name__ == "__main__":
    
    username = 'stockbroker'
    password = 'stock'
    databaseName = 'stocks'
    tableName = 'currentstocks'
    schema = 'stocks'


    importer = stockImporter(dbname=databaseName, user=username, tablename=tableName, schema=schema, password=password)
    
    try:
        importer.connect()
        tableCheck = importer.check_table_exists(table_name=tableName)
        if(tableCheck == False):
            importer.create_table()
            importer.begin_import()
        else:
            importer.begin_import()

    finally:
        importer.close_connection()


