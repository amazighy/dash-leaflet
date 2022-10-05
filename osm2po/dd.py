import pandas as pd
from time import time 
from sql import dd_in_ring
from config import con, list_of_stores

def execute_SQL2 (sql:str):
        con.execute(sql)

def read_SQL(sql):
    return pd.read_sql(sql, con)


def log_text(l):
    with open('logs.txt', 'a') as f:
        f.write(l)


if __name__=='__main__':
    total_tile = time()
    row_tracker = 0
    print("{:20.20}".format("Store ID #:"),"{:20.20}".format(str('Execu Time: ')), "{:20.20}".format(str('Batch ID:')), "{:25.25}".format(str('Inserted Centroids:')), "{:25.25}".format(str('Didnt_Route')),  "{:20.80}".format("Execption If Any: "))
    log_text("Store_ID,"+'Execu_Time,'+'Batch_ID'+'Inserted_Centroids,'+'Didnt_Route'+"Execption_If_Any"+"\n")
    print(106*'-')
    # log_text("\n")
    for store in list_of_stores :
        t = time()
        rings = [[0, 50]]
        while 0 < len(rings): 
            # rings = calculate_distance(store, rings, num_rows-row_tracker)
            failed_list = []
            for ring in rings:
                t1 = time()
                try :
                    execute_SQL2(dd_in_ring(store, ring[0]*1000, ring[1]*1000))  
                    num_rows = read_SQL(f"""select count(*) as counts from work_schema.drive_distance """)['counts'].to_list()[0] 
                    num_nodes = read_SQL(f"""SELECT  count(distinct hhp_pts.id_pt) as counts FROM osmrouting_north_america_us.centroid_nearest_nodes hhp_pts JOIN _considered_osm_nodes buffer ON ST_Intersects(buffer.geom, hhp_pts.the_geom_pt)""")['counts'].to_list()[0] 
                    print("{:20.20}".format(str(store)), "{:20.6}".format(str(round((time()-t1)/60,2))+' m'),"{:20.20}".format(str(ring)), "{:25.25}".format(str(num_rows-row_tracker)), "{:25.25}".format(str(num_nodes-(num_rows-row_tracker))),"{:20.80}".format("None"))
                    log_text(str(store)+','+str(round((time()-t1)/60,2))+','+str(ring[0]+ring[1])+','+str(num_rows-row_tracker)+','+str(num_nodes-(num_rows-row_tracker))+','+"None"+"\n")
                except Exception as e: 
                    failed_list.append([ring[0],round(sum(ring), 2) / len(ring)])
                    failed_list.append([round(sum(ring) / len(ring), 2), ring[1]])
                    print("{:20.20}".format(str(store)),"{:20.6}".format(str(round((time()-t1)/60,2))+' m'),"{:20.20}".format(str(ring)),  "{:25.25}".format(str(num_rows-row_tracker)),  "{:20.80}".format(repr(e)))
                    log_text("{:20.20}".format(str(store))+"{:20.6}".format(str(round((time()-t1)/60,2))+' m')+"{:20.20}".format(str(ring[0]+ring[1]))+"{:25.25}".format(str(num_rows-row_tracker))+"{:20.80}".format(repr(e))+"\n")
                    continue
            rings = failed_list
            row_tracker = num_rows

    log_text("\n")
    print( 'Total time '+str(round((time()-total_tile)/60,2)))

