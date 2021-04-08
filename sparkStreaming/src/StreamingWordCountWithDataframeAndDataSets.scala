import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger


object StreamingWordCountWithDataframeAndDataSets extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming App")
    .config("spark.sql.shuffle.partitions",4)
//    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .getOrCreate()
    
    //1. Read from the stream
    val streamDf = spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port","9998")
    .load
    
    //2. Process
    val wordDf  = streamDf.selectExpr("explode(split(value,' ')) as word")
    val countDf = wordDf.groupBy("word").count()

    
    //3. Write it  back to the sink
    val wordCountQuery = countDf.writeStream
    .format("console")
    .outputMode("complete")//other output modes include update and append
    .option("checkpointLocation", "checkpointlocation")
    .trigger(Trigger.ProcessingTime("30 seconds")) //if you do not give this it is going to process as soon as a line comes in
    .start()
    
    wordCountQuery.awaitTermination()
    
}