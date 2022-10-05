
import pgdumplib
import pandas as pd
import wget
import os
from time import time 

import subprocess
import psycopg 
from enc_password import decryptPassword 

DB_NAME = 'postgres'  # your db name

DB_USER = 'amazigh' # you db user
DB_HOST = "127.0.0.1"
DB_PASSWORD = 'it is me'# your db password


backup_file = 'osmrouting_central-america_cuba_latest_compressed.dump' 

dbname = "geodb"
host = "134.150.33.122"
user = "postgres"
port = 44444 
pgp = decryptPassword('~/.tdpasswordkey', "~/.geodbpw" )


t = time()
# print('downloding data for the whole USA')
# filepath = f'osmrouting_north-america_us_latest_compressed.dump'
# url = f'https://gis.cybertec-postgresql.com/osmrouting/north-america/us/osmrouting_north-america_us_latest_compressed.dump'

# filename = wget.download(url)
# print('\n Finished dowloading in :'+str(round((time()-t)/60,2)) )

try:
    process = subprocess.Popen(
                    ['pg_restore',
                        '--no-owner',
                        '--dbname=postgresql://{}:{}@{}:{}/{}'.format(
                            user,  #db user
                            pgp,   #db password
                            host,  #db host
                            port,  #db port
                            dbname #db name
                            ),  
                        '-v',
                        'osmrouting_north-america_us_latest_compressed.dump'],
                    stdout=subprocess.PIPE
                )
    # output = process.communicate()[0]
    

except Exception as e:
    print('Exception during restore %e' %(e) )

print('restored succesfully')
print('\n Finished restoring in :'+str(round((time()-t)/60,2)) )

# print('removing', filepath)

# if os.path.exists(filepath):
#     os.remove(filepath)
#     print('removed: ', filepath)
# else:
#     print("The file does not exist")
