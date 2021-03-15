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

object DataFrameWrite extends App {
  
  val sparkConf = new SparkConf()
  .set("spark.app.name", "My Application 1")
  .set("spark.master","local[2]")
  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
  
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
  
  print("current partitions is before repartition "+ordersDf.rdd.getNumPartitions)
  val ordersRep = ordersDf.repartition(1)
  print("current partitions is after repartition " +ordersRep.rdd.getNumPartitions)
  
  
  ordersRep.write.format("csv")
          .partitionBy("order_status")
          .mode(SaveMode.Overwrite)
          .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/t")
          .option("maxRecordsPerFile",2000)
          .save()
          
  scala.io.StdIn.readLine()
  spark.stop()
    
  
}