/* sparktest.scala */
import org.apache.spark.sql.SparkSession 
import org.apache.spark._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._ 
import org.apache.spark.sql._
import org.apache.log4j._
object spark_xmedia {
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
	import org.apache.spark.sql.types._	
val node_name = args(0)
val startReadTime = System.nanoTime()
val tv_df = spark.read.parquet("galactus/tunes_data_adj")
tv_df.createOrReplaceTempView("tv_df")
val tvh_df = spark.read.parquet("galactus/tv_hierarchy")
tvh_df.createOrReplaceTempView("tvh_df")
val t3 =  spark.sql(s"SELECT  node_name,time_id,sum(local_15_minute_num_seconds) as rawmins, count(DISTINCT as_hh_no) as rawhhs,avg(mkt_universe_vertical_factor_impacted) as mktuvfaci, min(network_no) as  netno,min(Depth) as depth FROM tv_df JOIN tvh_df ON tv_df.web_id = tvh_df.web_id WHERE tvh_df.node_name in ($node_name) group by node_name,time_id  order by time_id,node_name ").cache()
t3.createOrReplaceTempView("t3")
val t33 =  spark.sql("select node_name, sum(rawmins) as raw_mins ,sum(rawhhs),CAST(sum(rawhhs*mktuvfaci) as DECIMAL(38,4) ) as proj_hhs, CAST(sum(rawhhs*mktuvfaci)/sum(rawhhs) as DECIMAL(38,4) ) as waf from t3 group by node_name").cache();
t33.createOrReplaceTempView("t33")
val t4 =  spark.sql(s"SELECT  node_name, count(DISTINCT as_hh_no) as rawhhs FROM tv_df, tvh_df WHERE tv_df.web_id = tvh_df.web_id AND tvh_df.node_name in ( $node_name) group by node_name  order by node_name ").cache()
t4.createOrReplaceTempView("t4")
val t44det =  spark.sql("select t33.node_name,raw_mins,waf,rawhhs,CAST(rawhhs*waf  as DECIMAL(38,4)) as proj_hhs  from t33, t4 WHERE t33.node_name = t4.node_name" ).cache(); 
t44det.createOrReplaceTempView("t44det")
val topwaf = spark.sql("SELECT CAST( SUM(proj_hhs)/SUM(rawhhs) as DECIMAL(38,4)) from t44det").first.getDecimal(0)
val node  = spark.sql("select node_name,proj_hhs from t44det").cache();
node.createOrReplaceTempView("node")

val t5 =  spark.sql(s"SELECT   count(DISTINCT as_hh_no) as rawhhs FROM tv_df, tvh_df WHERE tv_df.web_id = tvh_df.web_id AND tvh_df.node_name in ($node_name)  ").cache()
t5.createOrReplaceTempView("t5")

val top = spark.sql(s"SELECT  'P1' as Top, rawhhs*$topwaf as proj_hhs from t5").cache()
top.createOrReplaceTempView("top")
node.union(top).show(20,false)
val endReadTime = System.nanoTime()
val readTime = (endReadTime - startReadTime)/1000000000
println(s"time taken is  $readTime")
spark.stop	
  }
}
