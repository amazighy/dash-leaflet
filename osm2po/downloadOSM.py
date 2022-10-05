
import pgdumplib
import pandas as pd
import wget
import os
from time import time 

import psycopg 
from enc_password import decryptPassword 



# dbname = "geodb"
# host = "134.150.33.97"
# user = "postgres"
# port = 44444 
# pgp = decryptPassword('~/.tdpasswordkey', "~/.geodbpw" )

# con = psycopg.connect( host = host, user = user, dbname = dbname, port=port, password=pgp,
#         keepalives=1, keepalives_idle= 60, keepalives_interval= 15, keepalives_count=5, autocommit= True)

# pg_con = f"postgresql://{'postgres'}:{pgp}@{host}:{44444}/{dbname}"



# def execute_SQL (sql:str):
#         con.execute(sql)

# def read_SQL(sql):
#     return pd.read_sql(sql, con)



columns =  ['id','osm_id', 'osm_name','osm_meta','osm_source_id' ,'osm_target_id','clazz','flags' ,'source' ,'target' ,'km','kmh' ,'cost','reverse_cost','x1' ,'y1','x2' ,'y2','geom_way']
regions = [ 'midwest', 'northeast', 'south', 'west']


total_tile = time()
t = time()
print('downloding data for the whole USA')

url = f'https://gis.cybertec-postgresql.com/osmrouting/north-america/us/osmrouting_north-america_us_latest_compressed.dump'
filename = wget.download(url)
print('\n Finished dowloading in :'+str(round((time()-t)/60,2)) )

t = time()
print('Reading the file with pgdumplib')
dump = pgdumplib.load(filename)
print('Read the data in :'+str(round((time()-t)/60,2)) )

t = time()
print('Converting to dataframe')
df = pd.DataFrame([row for row in dump.table_data(f'osmrouting_north_america_us', 'osm_2po_4pgr')], columns = columns)
print('Finished conversion into dataframe :'+str(round((time()-t)/60,2)) )


t = time()
print('Writing data to postgis \n')
df.to_csv('us.csv')
# df.to_sql(region, con=pg_con, if_exists='replace', index=False)
print('Finished writing to postgis :'+str(round((time()-t)/60,2)) )

print('removing', 'osmrouting_north-america_us_latest_compressed.dump')
print(df.head())
if os.path.exists('osmrouting_north-america_us_latest_compressed.dump'):
    os.remove('osmrouting_north-america_us_latest_compressed.dump')
    print('removed: ', 'osmrouting_north-america_us_latest_compressed.dump')
else:
    print("The file does not exist")

print( 'Total time :'+str(round((time()-total_tile)/60,2)))




# total_tile = time()

# for region in regions:
#     t = time()
#     print('downloding data for', region)
#     filepath = f'osmrouting_north-america_us-{region}_latest_compressed.dump'
#     url = f'https://gis.cybertec-postgresql.com/osmrouting/north-america/us-{region}/{filepath}'
#     filename = wget.download(url)
#     print('Finished dowloading in :'+str(round((time()-t)/60,2)) )

#     t = time()
#     print('\n Reading the file with pgdumplib')
#     dump = pgdumplib.load(filename)
#     print('Read the data in :'+str(round((time()-t)/60,2)) )

#     t = time()
#     print('\n Converting to dataframe')
#     rows = []
#     for row in dump.table_data(f'osmrouting_north_america_us_{region}', 'osm_2po_4pgr'):
#         rows.append(row)
#     df = pd.DataFrame(rows, columns = columns)
#     print('Finished conversion into dataframe :'+str(round((time()-t)/60,2)) )
    

#     t = time()
#     print('Writing data to postgis \n')
#     df.to_csv(region+'.csv')
##     df.to_sql(region, con=pg_con, if_exists='replace', index=False)
#     print('Finished writing to postgis :'+str(round((time()-t)/60,2)) )

#     print('removing', filepath)
#     print(df.head())
#     if os.path.exists(filepath):
#         os.remove(filepath)
#         print('removed: ', filepath)
#     else:
#         print("The file does not exist")

# print( 'Total time :'+str(round((time()-total_tile)/60,2)))



