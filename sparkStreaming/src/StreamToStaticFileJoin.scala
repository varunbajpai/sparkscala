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
import org.apache.spark.sql.types.LongType


object StreamToStaticFileJoin extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming App")
    .config("spark.sql.shuffle.partitions",4)
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .getOrCreate()
    
    //data example
    //{"card_id":5285400498362679,"amount":7982864,"postcode":92382,"pos_id":319523249711457,"transaction_dt":"2018-05-02 20:05:24"}
    //{"card_id":5285400498362679,"amount":2140506,"postcode":25040,"pos_id":762919765568319,"transaction_dt":"2014-01-08 03:31:52"}
    val schema = StructType(
        List(StructField("card_id",LongType),StructField("amount",IntegerType),StructField("postcode",IntegerType),
            StructField("pos_id",LongType),StructField("transaction_dt",TimestampType)))
            
     val transactionDf = spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", "8889")
    .load()
    
    val valueDf = transactionDf.select(from_json(col("value"),schema).alias("value"))
    val refinedDf = valueDf.select("value.*")
    refinedDf.printSchema()
    
    //Load Static Dataframe, df is as shown below
    //card_id,member_id,card_issue_date,country,state
    //5572427538311236,976740397894598,2014-12-15 08:06:58.0,United States,Tonawanda
    //5134334388575160,978465390240911,2012-10-17 11:55:14.0,United States,Kankakee
    
    val memberdf = spark.read
    .format("csv")
    .option("header",true)
    .option("inferSchema",true)
    .option("path","/Users/vbajpai/Downloads/trendy tech bigdata/temp/details.csv")
    .load()
    
    print(memberdf)
    
    val joinexpr = refinedDf.col("card_id") === memberdf.col("card_id")
    val jointype = "inner"
    
    val enricheddf = refinedDf.join(memberdf,joinexpr,jointype).drop(memberdf.col("card_id"))
    
    
    //print output to console, or write to the sink
     val joinquery = enricheddf.writeStream
    .format("console")
    .outputMode("update") //if output mode is complete watermark is overridden, so it will not allow us to clean the state store
    //spark allows to use append mode when we are using watermarks on event time
    //append will only print when the window is being discarded, since appennd does not allow updates
    .option("checkpointLocation", "chkpntloc22")
    .trigger(Trigger.ProcessingTime("15 second"))
    .start
    
    
    joinquery.awaitTermination()
    
    
}