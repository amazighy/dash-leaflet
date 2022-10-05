def dd_in_ring(store, inner_circle, outer_circle):
    return f"""
drop table if exists _considered_osm_nodes;
create temp table _considered_osm_nodes as
select  
    id_ss, 
    ST_Difference(
            ST_Buffer(the_geom_ss::geography, {outer_circle}*1.6)::geometry, 
            ST_Buffer(the_geom_ss::geography, {inner_circle}*1.6)::geometry 
    ) as geom
from   osmrouting_north_america_us.stores_nearest_nodes  where loccd =  '{store}';
   
_considered_osm_nodes
   
drop table if exists _considered_osm_edges;
create temp table _considered_osm_edges as
SELECT  id,
        osm_id as edge,
        osm_source_id as source,
        osm_target_id as target,
            cost,
        reverse_cost
FROM osmrouting_north_america_us.osm_2po_4pgr ways
JOIN (
    select  
            ST_Buffer(the_geom_ss::geography,{outer_circle+5000}*1.6)::geometry as geom
    from   osmrouting_north_america_us.stores_nearest_nodes 
    where loccd =   '{store}'
) ring on ring.geom  && ways.geom_way;


drop table if exists dijkstra;
create temp table dijkstra as
    select * from 
     pgr_dijkstra('SELECT  * FROM _considered_osm_edges',
                (
                    select array_agg(id_pt) 
                    from 
                        (
                        SELECT  distinct(hhp_pts.id_pt)
                        FROM osmrouting_north_america_us.centroid_nearest_nodes hhp_pts
                        JOIN _considered_osm_nodes buffer
                        ON ST_Intersects(buffer.geom, hhp_pts.the_geom_pt)
                        ) as utilized_nodes
            
                ), 
                (
                        select  id_ss from _considered_osm_nodes
                )
            );
    

insert into work_schema.drive_distance
    select 
    '{store}' as store,
    dijkstra.start_vid, 
    count(dijkstra.start_vid) as num_nodes,
    sum(edges.cost) as sum_seconds, 
    sum(edges.km) as sum_meters, 
    ST_Union(edges.geom_way) as geom
from  dijkstra
join osmrouting_north_america_us.osm_2po_4pgr edges on dijkstra.edge = edges.id
group by dijkstra.start_vid;
"""


print(dd_in_ring('TX0235', 0, 5000))

# # OR0161
# # AL0174
# # NY514
# # FL0012
# # TX0235