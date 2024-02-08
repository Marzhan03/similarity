
import psycopg2
import dotenv, os
dotenv.load_dotenv()

class DAL:
    def __init__(self):
        self.DATABASE_NAME = os.getenv("DATABASE_NAME")
        self.DATABASE_USER = os.getenv("DATABASE_USER")
        self.DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
        self.DATABASE_HOST = os.getenv("DATABASE_HOST")

        self.connection = None
        self.cursor = None

    def connection_open(self):
        self.connection = psycopg2.connect(dbname = self.DATABASE_NAME, user = self.DATABASE_USER, 
                    password = self.DATABASE_PASSWORD, host = self.DATABASE_HOST)
        self.cursor = self.connection.cursor()

    def connection_close(self):
        self.connection.close()

    def insert(self, table, column_names, *values):
        placeholders = ', '.join(['%s' for _ in values])
        # columns = ', '.join(column_names)
        query = f"INSERT INTO {table} ({column_names}) VALUES ({placeholders})"

        self.cursor.execute(query, values)
        self.connection.commit()

    def update(self, table, *values, **new_values):
        pass

    def delete(self, table, *values, **new_values):
        pass

    def last_inserted_data(self):
        self.cursor.execute("SELECT MAX(id) FROM similarity_news;")
        last_inserted_id = self.cursor.fetchone()[0]
        return last_inserted_id if last_inserted_id is not None else 0
    
    def select(self, table, fields, where=None, fetch=True):
        if where:
            self.cursor.execute("""
                SELECT %s FROM %s WHERE %s
            """%(fields, table, where))
        else:
            self.cursor.execute("""
                SELECT %s FROM %s
            """%(fields, table))

        if fetch == True:
            records = self.cursor.fetchall()
        else:
            records = self.cursor.fetchone()  
        self.connection.commit()
        return records
