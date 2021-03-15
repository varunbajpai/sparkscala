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

object SimpleAggregations extends App{
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
  invoiceDf.printSchema()
  invoiceDf.show()
  
  invoiceDf.select(
  count("*").as("TotalRowCount"),
  sum("Quantity").as("TotalQuantity"),
  avg("UnitPrice").as("AvgPrice"),
  countDistinct("InvoiceNo").as("CountDistinct")
  ).show
  
  
  invoiceDf.selectExpr(
  "count(*) as RowCount", 
  "sum(Quantity) as TotalQuantity",
  "avg(UnitPrice) as AvgPrice",
  "count(Distinct(InvoiceNo)) as CountDistinct"
  ).show
  
  invoiceDf.createOrReplaceTempView("sales") 
  spark.sql("select count(*),sum(Quantity),avg(UnitPrice),count(distinct(InvoiceNo)) from sales").show
  
  spark.stop()
  
}