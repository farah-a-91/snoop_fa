import psycopg2
from psql_conn import DB_CONNECTION


def insert_data_to_postgres(table_name, data_frame, id_column):
    """
    Insert data from a Pandas DataFrame into a PostgreSQL table.
    """
    try:
        # Establish a connection to the PostgreSQL database using the provided connection details
        conn = psycopg2.connect(**DB_CONNECTION)
        cur = conn.cursor()

        for index, row in data_frame.iterrows():
            cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {id_column} = %s", (row[id_column],))
            count = cur.fetchone()[0]

            if count == 0:
                cur.execute(f"INSERT INTO {table_name} VALUES %s", (tuple(row),))
            else:
                # Print a message for skipping insertion if a duplicate is found
                print(f"Skipping insertion for duplicate {id_column}: {row[id_column]}")

        conn.commit()
        cur.close()
        conn.close()
        print(f"Data inserted into {table_name} table successfully.")

    except Exception as e:
        # Print an error message if an exception occurs during the try block
        print(f"Error inserting data into {table_name}: {e}")
