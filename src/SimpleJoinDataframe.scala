import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SimpleJoinDataframe extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  .set("spark.app.name", "My Application 1")
  .set("spark.master","local[2]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
  
 val orderDf = spark.read
              .format("csv")
              .option("header",true)
              .option("InferSchema",true)
              .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/orders.csv")
              .load()
//  orderDf.show 
 val customerDf = spark.read
              .format("csv")
              .option("header",true)
              .option("InferSchema",true)
              .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/customers.csv")
              .load()
  
//  customerDf.show 
  val join_condition = orderDf.col("order_customer_id")===customerDf.col("customer_id")
  val joinquery = orderDf.join(customerDf,join_condition,"outer").sort("customer_id").withColumn("order_customer_id", expr("coalesce(order_customer_id,-1)"))
  joinquery.show(100)
  spark.stop()
  
}