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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.DateType

object GroupingAggregates extends App{
    val sparkConf = new SparkConf()
  .set("spark.app.name", "My Application 1")
  .set("spark.master","local[2]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
 
   val invoiceDf = spark.read
                  .format("csv")
                  .option("header",true)
                  .option("InferSchema",true)
                  .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/order_data.csv")
                  .load()
  
  invoiceDf.groupBy("Country", "InvoiceNo").agg(sum("Quantity").as("Total Qty"),
      sum(expr("Quantity * UnitPrice")).as("InvoiceValue")).show                
  
      
  invoiceDf.groupBy("Country", "InvoiceNo")
  .agg(expr("sum(Quantity) as TotalQuantity"),
      expr("sum(Quantity * UnitPrice) as InvoiceValue") )   
      
  invoiceDf.createOrReplaceTempView("sales")
  spark.sql("select Country, InvoiceNo, sum(Quantity) as TotalQuantity,sum(Quantity*UnitPrice) as InvoiceValueSql from sales group by Country,InvoiceNo").show
  
  //stopping the spark session                
  spark.stop()                
}