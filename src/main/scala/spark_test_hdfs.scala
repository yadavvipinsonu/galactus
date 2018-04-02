/* sparktest_hdfs.scala */
import org.apache.spark.sql.SparkSession 
import org.apache.spark._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._ 
import org.apache.spark.sql._
import org.apache.log4j._
object spark_test_hdfs {
  def main(args: Array[String]) {
	val spark = SparkSession
		.builder
		.appName("FirstApp")
		.master("yarn")
		.config("spark.sql.warehouse.dir", "file:///C:/temp")
		.enableHiveSupport()
		.getOrCreate()
	/*val sc = new SparkContext()*/	
	import spark.implicits._	 
	val AWS_ACCESS_KEY ="AKIAJXLBJ4MONQ7AVRYQ"
val AWS_SECRET_KEY ="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"
spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AWS_ACCESS_KEY)
spark.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", AWS_SECRET_KEY)
spark.sparkContext.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
val start_dt = "'" + args(0) + "'"
val end_dt = "'" + args(1)  + "'"
val network_group = "'" + args(2) + "'"
val network = args(3)
val episode = "'" + args(4) + "'"
val program = "'" + args(5) + "'"
val new_repeat =  "'" + args(6) + "'"
val demo_group1 = "'" + args(7) + "'"
val demo_group2 = "'" + args(8) + "'"
val daypart_label = "'" + args(9) + "'"
val timeframe_id = "'" + args(10) + "'"
val timeframe_label = "'" + args(11) + "'"
val platform = "'" + args(12) + "'"
val content_type = "'" + args(13) + "'"
val startReadTime = System.nanoTime()
val venti_hdfs = spark.read.parquet("hdfs:///venti_hdfs").filter($"start_dt" >= "2016-11-21" && $"start_dt" <= "2016-11-27" && $"network_group" === "NBC (TV)").createOrReplaceTempView("venti_hdfs")
val endReadTime = System.nanoTime()
val readTime = (endReadTime - startReadTime)/1000000000
val platform_map = spark.read.format("com.databricks.spark.csv").
	option("delimiter", ",").
	option("header", "true").
	option("inferSchema","true").
	load("s3a://csxpdev-xp-east/etv/imports/platform_map.csv")
platform_map.createOrReplaceTempView("platform_map")
val startTimeQuery = System.nanoTime()
spark.sql(s"""
select 
 A.timeframe_label
,A.network
,A.program
,A.episode
,A.new_repeat
,A.content_type
,A.daypart_label
,A.platform
,A.demo_group
,A.api_minutes
,A.api_reach
,A.api_reach_index
,A.runtime
,A.percent_reach
,A.average_audience
,B._ORDER_api_reach
from (
select timeframe_label as timeframe_label
	  , network as network
	  , program as program
	  , episode as episode
	  , new_repeat as new_repeat
	  , content_type as content_type
	  , daypart_label as daypart_label
	  , platform as platform
	  , demo_group as demo_group
	  , round(sum(min_proj*multiplier),0) as api_minutes
	  , round(sum(uv_proj*multiplier),0) as api_reach
	  , round( (sum(uv_proj*multiplier) / max(tv_ue)) / (sum(total_uv_proj*multiplier) / max(total_tv_ue)),2) as api_reach_index
	  , sum(runtime) as runtime
	  , round( (max(uv_proj) / max(total_uv_proj)) * 100, 2) as percent_reach
	  , round( max(min_proj) / max(uv_proj) , 2) as average_audience
	  from venti_hdfs vd
		   inner join platform_map pm on vd.base_platform = pm.base_platform
	  Where   (   network_group in ( $network_group )
	  and episode in ( $episode )  
	  and timeframe_id = $timeframe_id     
	  and timeframe_label in 
	  ( $timeframe_label )     
	  and start_dt between $start_dt and $end_dt     
	  and network_group in ( $network_group )  
	  and network like ( '%$network%' )
	  and program not in ( $program )  
	  and episode in ( $episode )   
	  and new_repeat in ( $new_repeat )  
	  and daypart_label in ( daypart_label )  
	  and (           demo_group in ( $demo_group1 )        or demo_group in ( $demo_group2 )     )    
	  and platform in ( $platform )   
	  and content_type in ( $content_type )  ) 
	   group by timeframe_label, network, program, episode, new_repeat, content_type, daypart_label, platform, demo_group
	) A
	 inner join (
	  select timeframe_label as timeframe_label
	  , network as network
	  , program as program
	  , episode as episode
	  , new_repeat as new_repeat
	  , content_type as content_type
	  , daypart_label as daypart_label
	  , platform as platform
	  , round(sum(total_uv_proj*multiplier),0) as _ORDER_api_reach
	  from venti_hdfs vd
		   inner join platform_map pm on vd.base_platform = pm.base_platform 
		Where   (   network_group in ( $network_group )
	  and episode in ( $episode )  
	  and timeframe_id = $timeframe_id     
	  and timeframe_label in 
	  ( $timeframe_label )     
	  and start_dt between $start_dt and $end_dt     
	  and network_group in ( $network_group ) 
	  and network like ( '%$network%' )
	  and program not in ( $program )  
	  and episode in ( $episode )   
	  and new_repeat in ( $new_repeat )  
	  and daypart_label in ( $daypart_label )  
	  and (           demo_group in ( $demo_group1 )        or demo_group in ( $demo_group2 )     )    
	  and platform in ( $platform )   
	  and content_type in ( $content_type )  )
	  and     demo_group in ( $demo_group1 )
	   group by timeframe_label, network, program, episode, new_repeat, content_type, daypart_label, platform
	   order by _ORDER_api_reach desc
	   limit 10 
	) 
	B 
	on A.timeframe_label = B.timeframe_label
	and A.network = B.network
	and A.program = B.program
	and A.episode = B.episode
	and A.new_repeat = B.new_repeat
	and A.content_type =B.content_type 
	and A.daypart_label = B.daypart_label 
	and A.platform = B.platform 
	order by api_reach desc""").show()	
val endTimeQuery = System.nanoTime()
val runTimeQuery = (endTimeQuery - startTimeQuery)/1000000000
val startCountTime = System.nanoTime()
spark.sql("select count(*) from venti_hdfs").show()
val endCountTime = System.nanoTime()
val countTime = (endCountTime - startCountTime)/1000000000
println(s"time taken by hdfs to read venti data is $readTime , to count records is $countTime and to execute sql query is $runTimeQuery seconds")
spark.stop	
  }
}