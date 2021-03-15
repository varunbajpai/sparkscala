import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.sql.Timestamp
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.SaveMode

object SparkTables extends App{
  val sparkConf = new SparkConf()
  .set("spark.app.name", "My Application 1")
  .set("spark.master","local[2]")
  val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
  
  val ordersSchema = StructType(List(
      StructField("order_id",IntegerType),StructField("order_date",TimestampType),
      StructField("order_customer_id",LongType),StructField("order_status",StringType)
      ))
  
 
   val ordersDf = spark.read
                      .format("csv")
                      .option("header",true)
                      .schema(ordersSchema)
                      .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/orders.csv")
                      .load()
  
  spark.sql("create database if not exists retail")
  
  ordersDf.write.format("csv")
          .mode(SaveMode.Overwrite)
          .bucketBy(4,"order_customer_id")
          .sortBy("order_customer_id")
          .saveAsTable("retail.orders")
          
  spark.catalog.listTables("retail").show()
          
  scala.io.StdIn.readLine()
  spark.stop()
}