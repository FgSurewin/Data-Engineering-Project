import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")


db_name = "project_master"
table_1_stage = "stage"
table_2_engine = "engine"
table_3_keyword = "keyword"
table_4_link = "link"
table_5_frequency = "frequency"


class MySQLService:
    def __init__(self) -> None:
        self.db_name = db_name
        self.table_1_stage = table_1_stage
        self.table_2_engine = table_2_engine
        self.table_3_keyword = table_3_keyword
        self.table_4_link = table_4_link
        self.table_5_frequency = table_5_frequency
        self.host = HOST
        self.port = PORT
        self.user = USER
        self.password = PASSWORD

    def create_engine_table(self):
        sql_statement_1 = f"""
              CREATE TABLE IF NOT EXISTS {self.db_name}.{self.table_2_engine}(
                engine_id INT NOT NULL AUTO_INCREMENT,
                engine_name TEXT NOT NULL,
                PRIMARY KEY (engine_id)
              );
              """
        self.update_db(sql_statement_1)

    def drop_engine_table(self):
        self.update_db("DROP TABLE IF EXISTS project_master.engine")
        print("Delete engine table successfully")

    def init_engine_table(self):
        search_engines = ["GOOGLE", "YAHOO", "BING"]
        sql_header = (
            f"INSERT INTO {self.db_name}.{self.table_2_engine}(engine_name) VALUES\n"
        )
        sql_values = ""
        for index, item in enumerate(search_engines):
            sql_values += "('" + item + "'),\n"

            if index == len(search_engines) - 1:
                sql_values = sql_values[:-2] + ";"

        sql_statement = sql_header + sql_values
        self.update_db(sql_statement)

    def create_stage_table(self):
        sql_statement_2 = f"""
              CREATE TABLE IF NOT EXISTS {self.db_name}.{self.table_1_stage}(
                result_id INT NOT NULL AUTO_INCREMENT,
                result_title TEXT NOT NULL,
                result_url TEXT NOT NULL,
                engine_id INT NOT NULL,
                is_ad BOOLEAN NOT NULL,
                created_at DATETIME NOT NULL,
                query_string TEXT NOT NULL,
                PRIMARY KEY (result_id),
                FOREIGN KEY(engine_id) REFERENCES {self.db_name}.{self.table_2_engine}(engine_id)
              );
              """
        self.update_db(sql_statement_2)

    def create_keyword_table(self):
        sql_statement_2 = f"""
              CREATE TABLE IF NOT EXISTS {self.db_name}.{self.table_3_keyword}(
                keyword_id VARCHAR(50) NOT NULL,
                text TEXT NOT NULL, 
                PRIMARY KEY (keyword_id)
              );
              """
        self.update_db(sql_statement_2)

    def create_link_table(self):
        sql_statement_2 = f"""
              CREATE TABLE IF NOT EXISTS {self.db_name}.{self.table_4_link}(
                link_id VARCHAR(50) NOT NULL,
                link_title TEXT NOT NULL,
                link_url TEXT NOT NULL,
                engine_id INT NOT NULL,
                is_ad BOOLEAN DEFAULT FALSE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                click_time INT NOT NULL, 
                PRIMARY KEY (link_id),
                FOREIGN KEY(engine_id) REFERENCES {db_name}.{self.table_2_engine}(engine_id)
              );
              """
        self.update_db(sql_statement_2)

    def create_frequency_table(self):
        sql_statement_2 = f"""
              CREATE TABLE IF NOT EXISTS {self.db_name}.{self.table_5_frequency}(
                freq_id INT NOT NULL AUTO_INCREMENT,
                keyword_id VARCHAR(50) NOT NULL, 
                link_id VARCHAR(50) NOT NULL,
                freq_value INT NOT NULL,
                word TEXT NOT NULL,
                PRIMARY KEY (freq_id),
                FOREIGN KEY(keyword_id) REFERENCES {self.db_name}.{self.table_3_keyword}(keyword_id),
                FOREIGN KEY(link_id) REFERENCES {self.db_name}.{self.table_4_link}(link_id)
              );
              """
        self.update_db(sql_statement_2)

    def drop_stage_table(self):
        self.update_db("DROP TABLE IF EXISTS project_master.stage ")
        print("Delete stage table successfully")

    def read_db(self, query):
        """Connect to AWS RDS MySQL database server"""

        conn = None
        # output = []
        try:

            # Connect to server
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )

            # Get a cursor
            cur = conn.cursor()

            # Execute a query
            cur.execute(query)

            # Fetch the result from the query
            output = cur.fetchall()
            print("Success reading from database!")

            # Close the communication with MySQL
            cur.close()

        except Exception as e:
            print("Error encountered: ", str(e))

        finally:
            if conn is not None:
                conn.close()
            print("Database connection closed.")

        return output

    def update_db(self, query):
        """Connect to AWS RDS MySQL database server"""

        conn = None

        try:

            # Connect to server
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
            )

            # Get a cursor
            cur = conn.cursor()

            # Execute a query
            cur.execute(query)
            print("Success updating database!")

            # Commit the changes to the database
            conn.commit()

            # Close the communication with MySQL
            cur.close()

        except Exception as e:
            print("Error encountered: ", str(e))

        finally:
            if conn is not None:
                conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    mysql_con = MySQLService()
    sql_command = f"""
    select * from project_master.stage WHERE query_string='what is mars' ORDER BY result_id DESC LIMIT 30
    """
    table_res = mysql_con.read_db(sql_command)
    print(table_res[:5])
    # print(mysql_con.password)
    # print(mysql_con.host)
    # print(mysql_con.port)
    # print(mysql_con.db_name)
