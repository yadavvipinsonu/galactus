/* sparktest.scala */
import org.apache.spark.sql.SparkSession 
import org.apache.spark._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._ 
import org.apache.spark.sql._
import org.apache.log4j._
object person_level {
  def main(args: Array[String]) {
	val spark = SparkSession
		.builder
		.appName("Personification")
		.master("yarn")
		.config("spark.sql.warehouse.dir", "file:///C:/temp")
		.enableHiveSupport()
		.getOrCreate()
	/*val sc = new SparkContext()*/
	import spark.implicits._
	import org.apache.spark.sql.types._	
 	val node_name = args(0) 
val startReadTime = System.nanoTime()

val etv_esp_live_tv_hwe_929w = spark.read.parquet("hdfs:///user/viyadav/galactus/etv_esp_live_tv_hwe_929w_v1")
etv_esp_live_tv_hwe_929w.createOrReplaceTempView("etv_esp_live_tv_hwe_929w")
val tvh_df = spark.read.parquet("hdfs:///user/viyadav/galactus/tv_hierarchy")
tvh_df.createOrReplaceTempView("tvh_df")
val tunes_data = spark.read.parquet("hdfs:///user/viyadav/galactus/tunes_full")
tunes_data.createOrReplaceTempView("tunes_data")
val tag_details = spark.read.parquet("hdfs:///user/viyadav/galactus/tag_details_parquet")
tag_details.createOrReplaceTempView("tag_details")
val tag_hhs_weight = spark.read.parquet("hdfs:///user/viyadav/galactus/tag_hhs_weight")
tag_hhs_weight.createOrReplaceTempView("tag_hhs_weight")
val tag_hhs  = spark.read.parquet("hdfs:///user/viyadav/galactus/tag_hhs")
tag_hhs.createOrReplaceTempView("tag_hhs")
val networks = spark.read.parquet("hdfs:///user/viyadav/galactus/networks")
networks.createOrReplaceTempView("networks")
val series = spark.read.parquet("hdfs:///user/viyadav/galactus/series")
series.createOrReplaceTempView("series")
val stations = spark.read.parquet("hdfs:///user/viyadav/galactus/stations")
stations.createOrReplaceTempView("stations")

/*t44 upper_check = select * , INSTR(node_name, Name1) as check_name1,INSTR(node_name, Name2) as check_name2,....5 , CASE WHEN INSTR(node_name, Name1) <> 0 then INSTR(node_name, Name1) ,CASE WHEN INSTR(node_name, Name2) <> 0 , INSTR(node_name, Name2) ))) from */
val t3 =  spark.sql(s"""SELECT  node_name,tvh_df.web_id,local_broadcast_day,sum(local_15_minute_num_seconds) as rawmins, count(DISTINCT as_hh_no) as rawhhs,
avg(mkt_universe_vertical_factor_impacted) as  uvf,avg(mkt_universe_factor_impacted) as  uf,  
count(DISTINCT as_hh_no)* avg(mkt_universe_vertical_factor_impacted) as proj_hhs, 
sum(local_15_minute_num_seconds)*avg(mkt_universe_factor_impacted) as proj_mins,
min(network_no) as  netno,min(Depth) as depth 
FROM tunes_data tv_df, tvh_df 
WHERE tv_df.web_id = tvh_df.web_id 
AND tvh_df.node_name in ($node_name)
group by node_name,tvh_df.web_id,local_broadcast_day  order by local_broadcast_day,node_name """)
t3.createOrReplaceTempView("t3")

/*val t3_t =  spark.sql(s"""SELECT  node_name,tvh_df.web_id,time_id,sum(local_15_minute_num_seconds) as rawmins, count(DISTINCT as_hh_no) as rawhhs,
avg(mkt_universe_vertical_factor_impacted) as  uvf,avg(mkt_universe_factor_impacted) as  uf,  
count(DISTINCT as_hh_no)* avg(mkt_universe_vertical_factor_impacted) as proj_hhs, 
sum(local_15_minute_num_seconds)*avg(mkt_universe_factor_impacted) as proj_mins,
min(network_no) as  netno,min(Depth) as depth 
FROM tunes_data tv_df, tvh_df 
WHERE tv_df.web_id = tvh_df.web_id 
AND tvh_df.node_name in ('SportsCenter 10/17 10:00P T (60) on ESPN','SportsCenter 10/17 07:00A T (60) on ESPN','SportsCenter 10/17 08:00A T (60) on ESPN','SportsCenter 10/17 09:00A T (60) on ESPN','SportsCenter 10/17 12:00P T (60) on ESPN','SportsCenter 10/17 11:00P T (60) on ESPN','SportsCenter 10/16 06:00A M (60) on ESPN','SportsCenter 10/16 07:00A M (60) on ESPN','SportsCenter 10/16 08:00A M (60) on ESPN','SportsCenter 10/16 09:00A M (60) on ESPN','SportsCenter 10/16 12:00P M (60) on ESPN')
group by node_name,tvh_df.web_id,time_id  order by time_id,node_name """)
t3_t.createOrReplaceTempView("t3_t")
*/

val t33 =  spark.sql("""select node_name,depth, sum(rawmins) as rawmins,sum(proj_mins) as proj_mins ,sum(rawhhs),CAST(sum(proj_hhs) as DECIMAL(38,4) ),
CAST(sum(proj_hhs) as DECIMAL(38,4) )/ sum(rawhhs) as waf  from t3 group by node_name,depth""").cache();
t33.createOrReplaceTempView("t33")
/*val mind = spark.sql("select min(depth) as minde  from t3").first.getInt(0) */
val t4 =  spark.sql(s"""SELECT  node_name, count(DISTINCT as_hh_no) as rawhhs ,Depth
		FROM tunes_data tv_df, tvh_df WHERE tv_df.web_id = tvh_df.web_id 
		AND tvh_df.node_name in ($node_name)
		group by node_name,Depth  order by node_name """).cache();
t4.createOrReplaceTempView("t4")
val t44det =  spark.sql("select t33.node_name,rawmins,cast(proj_mins/60 as long) as proj_mins ,waf,rawhhs,CAST(rawhhs*waf  as DECIMAL(18,4)) as proj_hhs  from t33, t4 WHERE t33.node_name = t4.node_name" ).cache(); 
t44det.createOrReplaceTempView("t44det")
val topwaf = spark.sql("SELECT CAST( SUM(proj_hhs)/SUM(rawhhs) as DECIMAL(38,4)) from t44det").first.getDecimal(0)
val node  = spark.sql("select node_name,proj_mins,proj_hhs from t44det").cache();
node.createOrReplaceTempView("node")

/*val t5 =  spark.sql(s"""SELECT node_name,sum(local_15_minute_num_seconds*mkt_universe_factor_impacted) as proj_mins, count(DISTINCT as_hh_no) as rawhhs 
FROM tunes_data tv_df, tvh_df WHERE tv_df.web_id = tvh_df.web_id AND tvh_df.node_name in ($node_name)
group by node_name""")

t5.createOrReplaceTempView("t5")

val top = spark.sql(s"SELECT  'P1' as Top, cast(proj_mins/60 as long) as proj_mins,rawhhs*$topwaf as proj_hhs from t5").cache()
top.createOrReplaceTempView("top") 
val hh_viwership = node.union(top)
hh_viwership.createOrReplaceTempView("hh_viwership")

*/
node.createOrReplaceTempView("hh_viwership")

val cum_df = spark.sql(s"""Select as_hh_no,node_name,CVF,VPVH,hh_weight,
cast(sum(hh_weight) over (partition by node_name order by vpvh,as_hh_no) as decimal(18,4))  as cum_sum_vpvh,
cast(sum(hh_weight) over (partition by node_name order by cvf,as_hh_no) as decimal(18,4))  as cum_sum_cvf,
cast(sum(hh_weight) over (partition by node_name ) as decimal(18,4))/2  as mid_sum
from
(select x.as_hh_no,x.node_name,
 count(distinct x.person_id ), 
 max(hh_minutes) as sum_hh_minutes,
 sum(reg_mdl_mins) as sum_person_mins,
 sum(reg_mdl_mins)/max(hh_minutes) as CVF,
 sum(case when x.agg_uv = 0 then 1 else  x.agg_uv end  ) as VPVH , 
 Min(x.hh_weight) as hh_weight
 from
 (
select as_hh_no,person_id,node_name,
  1- (pow(10,Sum(log_10_reg_mdl) ) ) as agg_uv,
  sum(reg_mdl_mins) as reg_mdl_mins,
  max(hh_minutes) as hh_minutes,
  max(hh_weight) as hh_weight
from etv_esp_live_tv_hwe_929w  a 
join tvh_df b 
on a.web_id = b.web_id where
node_name in ($node_name )
group by as_hh_no,person_id,node_name) x
group by x.as_hh_no, x.node_name
)
""")

cum_df.createOrReplaceTempView("cum_df")

/*val cvf_df = spark.sql(s"""
Select node_name,min(cvf) as cvf from
(Select as_hh_no,node_name,CVF,hh_weight,
cast(sum(hh_weight) over (partition by node_name order by cvf,as_hh_no) as decimal(18,4))  as cum_sum_cvf,
cast(sum(hh_weight) over (partition by node_name ) as decimal(18,4))/2  as mid_sum
from
(Select as_hh_no , node_name ,
sum(reg_mdl_mins) as reg_mdl_mins,
sum(hh_minutes) as hh_minutes,
min(hh_weight) as hh_weight,
sum(reg_mdl_mins)/sum(hh_minutes) as CVF
from
(
select as_hh_no,a.web_id,node_name,
sum(reg_mdl_mins) as reg_mdl_mins,
max(hh_minutes) as hh_minutes,
max(hh_weight) as hh_weight
from etv_esp_live_tv_hwe_929w  a
join tvh_df b
on a.web_id = b.web_id 
and node_name in ('Jimmy Kimmel Live! 10/16 11:35P M (62) on ABC','Jimmy Kimmel Live! 10/17 11:35P T (62) on ABC','Jimmy Kimmel Live! 10/18 11:35P W (62) on ABC','Jimmy Kimmel Live! 10/19 11:35P R (62) on ABC','Jimmy Kimmel Live! 10/20 11:35P F (62) on ABC','Jimmy Kimmel Live! 2017-10-16 on ABC','Jimmy Kimmel Live! 2017-10-17 on ABC','Jimmy Kimmel Live! 2017-10-18 on ABC','Jimmy Kimmel Live! 2017-10-19 on ABC','Jimmy Kimmel Live! 2017-10-20 on ABC','Jimmy Kimmel Live! on ABC' )
group by as_hh_no,a.web_id,node_name
) x
group by as_hh_no , node_name
)
)where cum_sum_cvf > mid_sum
group by node_name
""")
*/

val cvf_df = spark.sql(s"""
Select node_name,min(cvf) as cvf from
(Select as_hh_no,web_id,node_name,CVF,hh_weight,
cast(sum(hh_weight) over (partition by node_name order by cvf,as_hh_no) as decimal(18,4))  as cum_sum_cvf,
cast(sum(hh_weight) over (partition by node_name ) as decimal(18,4))/2  as mid_sum
from
(Select as_hh_no , web_id ,node_name,
sum(reg_mdl_mins) as reg_mdl_mins,
max(hh_minutes) as hh_minutes,
min(hh_weight) as hh_weight,
sum(reg_mdl_mins)/max(hh_minutes) as CVF
from
(
select as_hh_no,a.web_id,node_name,
sum(reg_mdl_mins) as reg_mdl_mins,
max(hh_minutes) as hh_minutes,
min(hh_weight) as hh_weight
from etv_esp_live_tv_hwe_929w  a
join tvh_df b
on a.web_id = b.web_id 
and node_name in ($node_name )
group by as_hh_no,a.web_id,node_name
) x
group by as_hh_no ,web_id, node_name
)
)where cum_sum_cvf > mid_sum
group by node_name
""")

cvf_df.createOrReplaceTempView("cvf_df")



val cvf_vpvh = spark.sql("""
Select a.node_name,a.cvf , b.vpvh from 
(
Select node_name,min(cvf)  as cvf from cvf_df 
group by node_name
) a 
Inner join (
Select node_name,min(vpvh) as vpvh from cum_df 
where cum_sum_vpvh > mid_sum
group by node_name) b
on a.node_name = b.node_name
""")

/*val cum_df_p = spark.sql(s"""Select as_hh_no,CVF,VPVH,hh_weight,
cast(sum(hh_weight) over (order by cvf,as_hh_no) as decimal(18,4))  as cum_sum_cvf,
cast(sum(hh_weight) over (order by vpvh,as_hh_no) as decimal(18,4))  as cum_sum_vpvh,
cast(sum(hh_weight) over ( ) as decimal(18,4))/2  as mid_sum
from
(select x.as_hh_no,
 count(distinct x.person_id ), 
 max(hh_minutes) as sum_hh_minutes,
 sum(reg_mdl_mins) as sum_person_mins,
 sum(reg_mdl_mins)/max(hh_minutes) as CVF,
 sum(case when x.agg_uv = 0 then 1 else  x.agg_uv end  ) as VPVH , 
 Min(x.hh_weight) as hh_weight
 from
 (
select as_hh_no,person_id,
  1- (pow(10,Sum(log_10_reg_mdl) ) ) as agg_uv,
  sum(reg_mdl_mins) as reg_mdl_mins,
  max(hh_minutes) as hh_minutes,
  max(hh_weight) as hh_weight
from etv_esp_live_tv_hwe_929w  a 
join tvh_df b 
on a.web_id = b.web_id where
node_name in ($node_name )
group by as_hh_no,person_id ) x
group by x.as_hh_no)
""") 
cum_df_p.createOrReplaceTempView("cum_df_p")

val cvf_df_p = spark.sql(s"""
Select 'P1' as node_name,min(cvf) as cvf from
(Select as_hh_no,web_id,node_name,CVF,hh_weight,
cast(sum(hh_weight) over ( order by cvf,as_hh_no) as decimal(18,4))  as cum_sum_cvf,
cast(sum(hh_weight) over ( ) as decimal(18,4))/2  as mid_sum
from
(Select as_hh_no , web_id ,node_name,
sum(reg_mdl_mins) as reg_mdl_mins,
max(hh_minutes) as hh_minutes,
min(hh_weight) as hh_weight,
sum(reg_mdl_mins)/max(hh_minutes) as CVF
from
(
select as_hh_no,a.web_id,node_name,
sum(reg_mdl_mins) as reg_mdl_mins,
max(hh_minutes) as hh_minutes,
min(hh_weight) as hh_weight
from etv_esp_live_tv_hwe_929w  a
join tvh_df b
on a.web_id = b.web_id 
and node_name in ($node_name )
group by as_hh_no,a.web_id,node_name
) x
group by as_hh_no ,web_id, node_name
)
)where cum_sum_cvf > mid_sum
""")



cvf_df_p.createOrReplaceTempView("cvf_df_p")

val cvf_vpvh_p = spark.sql("""
Select a.node_name,a.cvf , b.vpvh from 
(
Select 'P1' as node_name,min(cvf)  as cvf from cvf_df_p 
) a 
Inner join (
Select 'P1' as node_name,min(vpvh) as vpvh from cum_df_p 
where cum_sum_vpvh > mid_sum
group by 1) b
on a.node_name = b.node_name
""")



val projections = cvf_vpvh_p.union(cvf_vpvh)
projections.createOrReplaceTempView("projections")

*/
cvf_vpvh.createOrReplaceTempView("projections")

val personified_numbers  = spark.sql("""
Select 
a.node_name,
a.proj_mins,
a.proj_hhs,
b.cvf,
b.vpvh,
cast(a.proj_mins*b.cvf as long) as person_minutes,
cast(a.proj_hhs*b.vpvh as long) as person_reach
from hh_viwership a
inner join 
projections b
on a.node_name = b.node_name
""")
personified_numbers.show(1000,false)

val msd = new SVE()
val nd = new TVE()
msd.cal_market_station_day()
msd.series_day()
nd.cal_network_day()
nd.market_report()
val endReadTime = System.nanoTime()
val readTime = (endReadTime - startReadTime)/1000000000
println(s"time taken is  $readTime")
spark.stop	
  }
} 

