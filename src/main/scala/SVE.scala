class SVE {
import org.apache.spark.sql.SparkSession 
import org.apache.spark._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._ 
import org.apache.spark.sql._
import org.apache.log4j._

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
def cal_market_station_day() {
	
val market_station_day = spark.sql("""
Select tv_market_no,
network_no,
local_broadcast_day,
count(distinct as_hh_no) as reach,
count(distinct as_hh_no) * max(mkt_universe_vertical_factor) as projected_reach,
sum(local_15_minute_num_hours) as hh_hrs,
sum(local_15_minute_num_hours) * max(mkt_universe_factor) as projected_hrs,
sum(local_15_minute_num_hours) * max(mkt_universe_factor)/24 as AA,
(sum(local_15_minute_num_hours) * max(mkt_universe_factor)/24/max(universe))*100 as Rtg
from 
tunes_data where tv_market_no = 517
group by tv_market_no,network_no,local_broadcast_day
order by local_broadcast_day,network_no
""").show(100,false)

}
def series_day() 
{
spark.sql("""
Select tv_market_no,
network_no,
local_broadcast_day,
series_name,
local_airing_start_time,
runtime,
count(distinct as_hh_no) as reach,
count(distinct as_hh_no) * max(mkt_universe_vertical_factor) as projected_reach,
sum(local_15_minute_num_hours) as hh_hrs,
sum(local_15_minute_num_hours) * max(mkt_universe_factor) as projected_hrs,
sum(local_15_minute_num_hours) * max(mkt_universe_factor)/(runtime/60) as AA,
(sum(local_15_minute_num_hours) * max(mkt_universe_factor)/(runtime/60)/max(universe))*100 as Rtg
from 
tunes_data t
join series s
on t.series_no = s.series_no
where tv_market_no = 517
and local_broadcast_day = "2017-10-16"
group by tv_market_no,network_no,local_broadcast_day,series_name,local_airing_start_time,runtime
order by local_broadcast_day,network_no
""").show(300,false)
}

}