

import psycopg 
from enc_password import decryptPassword 
import pandas as pd


dbname = "geodb"
host = "134.150.33.97"
user = "postgres"
port = 44444 
pgp = decryptPassword('~/.tdpasswordkey', "~/.geodbpw" )

con = psycopg.connect( host = host, user = user, dbname = dbname, port=port, password=pgp,
        keepalives=1, keepalives_idle= 60, keepalives_interval= 15, keepalives_count=5, autocommit= True)

# parameters
def read_SQL(sql):
    return pd.read_sql(sql, con)



list_of_stores = read_SQL(f"select distinct (loccd) as stores from  osmrouting_north_america_us.stores_nearest_nodes")['stores'].to_list()
#read_SQL(f"select distinct (loccd) as stores from  work_schema.stores_nearest_nodes")['stores'].to_list()
# ['OR0161', 'AL0174', 'NY514', 'FL0012', 'TX0235']



list_of_rings =[[0, 50]]
