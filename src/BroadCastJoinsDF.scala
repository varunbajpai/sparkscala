import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object BroadCastJoinsDF extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  .set("spark.app.name", "My Application 1")
  .set("spark.master","local[2]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
 val schema_orders = StructType(
 List(
 StructField("order_id",IntegerType,true),
 StructField("order_date",TimestampType,true),
 StructField("order_customer_id",IntegerType,true),
 StructField("order_status",StringType,true)
 )    
 )
 val orderDf = spark.read
              .format("csv")
              .schema(schema_orders)
              .option("header",true)
//              .option("InferSchema",true)
              .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/orders.csv")
              .load()
//  orderDf.show 
 val customerDf = spark.read
              .format("csv")
              .option("header",true)
              .option("InferSchema",true) //dont infer schema it will take a lot of time, provide datatype using case class or something
              .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/customers.csv")
              .load()
  //by default spark tries to do a broadcast join so we need to disable it manually as shown below
 // spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
  //if it is not disabled then the join will be broadcast with the smaller df being broadcasted everywhere
  val join_condition = orderDf.col("order_customer_id")===customerDf.col("customer_id")
  val joinquery = orderDf.join(customerDf,join_condition,"outer").sort("customer_id").withColumn("order_customer_id", expr("coalesce(order_customer_id,-1)"))
  joinquery.show(100)
  joinquery.write.csv("/Users/vbajpai/Downloads/trendy tech bigdata/temp/outfile_for_join")
   scala.io.StdIn.readLine()
  spark.stop()
}