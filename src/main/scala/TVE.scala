class TVE {
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
def cal_network_day() {
	
spark.sql("""
Select
network_no,
local_broadcast_day,
count(distinct as_hh_no) as reach,
cast(count(distinct as_hh_no) * max(nat_universe_vertical_factor) as decimal(18,4)) as projected_reach,
sum(local_15_minute_num_hours) as hh_hrs,
cast(sum(local_15_minute_num_hours) * max(nat_universe_factor) as decimal(18,4)) as projected_hrs,
sum(local_15_minute_num_hours) * max(nat_universe_factor)/24 as AA,
(sum(local_15_minute_num_hours) * max(nat_universe_factor)/24/max(112143960))*100 as Rtg
from 
tunes_data 
group by network_no,local_broadcast_day
order by local_broadcast_day,network_no
""").show

}

def market_report() {
spark.sql("""
Select
tv_market_no,
local_broadcast_day,
count(distinct as_hh_no) as reach,
cast(count(distinct as_hh_no) * max(mkt_universe_vertical_factor) as decimal(18,4)) as projected_reach,
sum(local_15_minute_num_hours) as hh_hrs,
cast(sum(local_15_minute_num_hours) * max(mkt_universe_factor) as decimal(18,4)) as projected_hrs,
sum(local_15_minute_num_hours) * max(mkt_universe_factor)/24 as AA,
(sum(local_15_minute_num_hours) * max(mkt_universe_factor)/24/max(universe))*100 as Rtg
from 
tunes_data where local_broadcast_day = "2017-10-16" and network_no = 33
group by tv_market_no,local_broadcast_day
order by tv_market_no
""").show(300,false)

}
}