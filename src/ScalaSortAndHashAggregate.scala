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


//In this project we try to find how many orders are placed by  a customer in which month
object ScalaSortAndHashAggregate extends App{
  
    Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sparkConf = new SparkConf()
  .set("spark.app.name", "My Application 1")
  .set("spark.master","local[2]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
    val ordersDf = spark.read
                        .format("csv")
                        .option("header",true)
                        .option("inferSchema",true)
                        .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/orders.csv")
                        .load
   ordersDf.createOrReplaceTempView("orders_table")
   val rdd = spark.sql("select * from orders_table limit 20").show()
   
   //this is a slow query and is an example of sort aggregate
   val aggregate_query = spark.sql("select order_customer_id, date_format(order_date, 'MMMM') orderdt, count(1) cnt, first(date_format(order_date,'M')) monthnum from orders_table group by order_customer_id, orderdt order by cast(monthnum as int)")
   aggregate_query.show()

   //this is a fast query and is an example of hash aggregate   
   val aggregate_query2 = spark.sql("select order_customer_id, date_format(order_date, 'MMMM') orderdt, count(1) cnt, first(cast(date_format(order_date,'M') as int)) monthnum from orders_table group by order_customer_id, orderdt order by monthnum").show
  scala.io.StdIn.readLine()
  spark.stop()
}