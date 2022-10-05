
--@CONTEXT pg_geodb;

-------------------------------------------------------------------
---------------------- Creating the vertices table ----------------
-------------------------------------------------------------------

DROP TABLE IF EXISTS  osmrouting_north_america_us.vertices;
create table osmrouting_north_america_us.vertices as
	select left (ST_GeoHash (geom), 5) AS gh_ss, * from (
	select osm_source_id as osm_id ,  ST_StartPoint(geom_way) as geom FROM osmrouting_north_america_us.osm_2po_4pgr 
	union all
	select osm_target_id as osm_id , ST_EndPoint(geom_way) as geom   from osmrouting_north_america_us.osm_2po_4pgr
	)l
	group by osm_id, geom

create index osm_vertices_geom_index on osmrouting_north_america_us.vertices using gist(geom);
create index osm_vertices_id_index on osmrouting_north_america_us.vertices (osm_id);


-------------------------------------------------------------------
-------- Finding the neareast node for each Spectrum Store --------
-------------------------------------------------------------------

drop table if exists osmrouting_north_america_us.stores_nearest_nodes;
create table osmrouting_north_america_us.stores_nearest_nodes as
select l.loccd, l.pt,nn_ss.*, ST_Distance (l.pt::geography, nn_ss.the_geom_ss::geography) AS d_meters_ss  
from work_schema.ss_pt l
cross join LATERAL (
		select osm_id as id_ss, geom as the_geom_ss
		from
			osmrouting_north_america_us.vertices v3    
		where
			v3.geom && ST_Expand( l.pt  , 0.2) 
		order by v3.geom <->  l.pt   asc 
		limit 1 
	) nn_ss;


drop table if exists dd_tmp.hhp_work_table_1;
create table dd_tmp.hhp_work_table_1 as 
select svclocid, state, pt, gh, left(gh, 5) as gh_ss, left(gh, 6) as gh_6
from work_schema.locs_pt; 

create index hhp_rs1 on dd_tmp.hhp_work_table_1 (svclocid);
create index gh2_6 on dd_tmp.hhp_work_table_1 (gh_6);
create index hhp_pt1 on dd_tmp.hhp_work_table_1 using gist(pt);


drop table if exists dd_tmp.gh6;
create table dd_tmp.gh6 as

select gh_6,
	   ST_CENTROID(ST_COLLECT(pt)) as centroid, 
	   left(ST_Geohash(ST_CENTROID(ST_COLLECT(pt))),5) as gh_ss,
	   count(*) as count, 
	   count(distinct svclocid) as dstnct_slid, 
	   count(distinct pt) as dstnct_pt
from dd_tmp.hhp_work_table_1
group by 1

drop table if exists osmrouting_north_america_us.centroid_nearest_nodes;
create table osmrouting_north_america_us.centroid_nearest_nodes as
select l.*, nn.*, ST_Distance (l.centroid::geography, nn.the_geom_pt::geography) AS d_meters_pt
from dd_tmp.gh6  l 
	cross join LATERAL (
		select osm_id as id_pt, geom as the_geom_pt 
		from
			osmrouting_north_america_us.vertices v    
		where
			v.geom && ST_Expand( l.centroid  , .4) 
			and v.gh_ss = l.gh_ss
		order by v.geom <->  l.centroid  asc 
		limit 1 
	) nn;

create index id_pt_osm_id on osmrouting_north_america_us.centroid_nearest_nodes(id_pt);

