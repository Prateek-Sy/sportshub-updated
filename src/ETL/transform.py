from pyspark.sql import SparkSession
import psycopg2

def read_from_database(spark: SparkSession, db_config):

    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            user=db_config['user'],
            password=db_config['password']
        )
        
        print("Connected to PostgreSQL!")
        
        # Perform database operations here...

        cursor = conn.cursor()
        cursor.execute("SELECT id FROM game_fixture")
        fixture_ids = [row[0] for row in cursor.fetchall()]
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        table_names = [row[0] for row in cursor.fetchall()]

        
        # Dictionary to store DataFrames by table name
        dfs_dict = {}  
       
        # Read tables into Spark DataFrames
        for fixture_id in fixture_ids:
            for table_name in table_names:
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s AND column_name = 'fixture_id'", (table_name,))
                fixture_id_exists = cursor.fetchone()
                if fixture_id_exists:
                    properties = {
                        "url": db_config['url'],
                        "user": db_config['user'],
                        "password": db_config['password'],
                        "driver": db_config['driver']
                    }
                    if table_name == "game_match_event":
                        df = spark.read \
                            .format("jdbc") \
                            .options(**properties) \
                            .option("query", f"SELECT * FROM {table_name} WHERE fixture_id = {fixture_id}") \
                            .load()
                        print(f"df created for fixture id {fixture_id}")
                        df.createOrReplaceTempView("game_match_event")
    
    except psycopg2.Error as e:
        print("Error: Could not connect to PostgreSQL")
        print(e)
        
    finally:
        conn.close()
        cursor.close()
        print("Database Connection closed.")

    # print(dfs_dict)
    return dfs_dict