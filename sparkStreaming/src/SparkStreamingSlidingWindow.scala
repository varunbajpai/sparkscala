import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType


object SparkStreamingSlidingWindow extends  App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming App")
    .config("spark.sql.shuffle.partitions",4)
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .getOrCreate()

    
    //define schema
    val orderschema = StructType(List(
        StructField("order_id",IntegerType),
        StructField("order_date",TimestampType),
        StructField("order_customer_id",IntegerType),
        StructField("order_status",StringType),
        StructField("amount",IntegerType)
    ))
    
    
    //1. Read from the socket
    val ordersDf = spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", "8889")
    .load()
    
    val valuedf = ordersDf.select(from_json(col("value"),orderschema).alias("value"))
    
    val refinedordersdf = valuedf.select("value.*")
    
    val windowaggdf = refinedordersdf
    .withWatermark("order_date", "30 minute") //So that events that after execution dont come within 30 minutes are not counted in the calculation
    .groupBy(window(col("order_date"),"15 minute","5 minute")) //It is same as tumbling window just the 5 minute is the sliding window ooverlap argument thats it rest all is same
    .agg(sum("amount")
    .alias("totalInvoice"))
    
    val outdf = windowaggdf.select("window.start","window.end","totalInvoice")
    
    
    //write to the sink
    val ordersquery = outdf.writeStream
    .format("console")
    .outputMode("update") //if output mode is complete watermark is overridden, so it will not allow us to clean the state store
    //spark allows to use append mode when we are using watermarks on event time
    //append will only print when the window is being discarded, since appennd does not allow updates
    .option("checkpointLocation", "chkpntloc2")
    .trigger(Trigger.ProcessingTime("15 second"))
    .start
    
    ordersquery.awaitTermination()
}