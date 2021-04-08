import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object SparkStreamingReadFromFile extends App{
      Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming App")
    .config("spark.sql.shuffle.partitions",4)
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .config("spark.sql.streaming.schemaInference","true")
    .getOrCreate()
    
    //1. Read from the stream
    val ordersDf = spark.readStream.format("json")
    .option("path", "/Users/vbajpai/Downloads/trendy tech bigdata/temp/myinpfolder")
//    .option("maxFilesPerTrigger",1) this does some kind of load balancing
    .load
    
    //2. Process
    ordersDf.createOrReplaceTempView("orders")
    val completed_orders = spark.sql("select * from orders where order_status = 'COMPLETE'")
    
    
    //3. Write it  back to the sink
    val wordCountQuery = completed_orders.writeStream
    .format("json")
    .outputMode("append")
    .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/myoutfolder")
    .option("checkpointLocation", "checkpointlocation2")
    .trigger(Trigger.ProcessingTime("30 seconds")) 
    .start()
    
    wordCountQuery.awaitTermination()
}