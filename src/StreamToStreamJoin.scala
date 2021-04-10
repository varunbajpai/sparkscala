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


object StreamToStreamJoin extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder()
    .master("local[2]")
    .appName("My Streaming App")
    .config("spark.sql.shuffle.partitions",4)
    .config("spark.streaming.stopGracefullyOnShutdown","true")
    .getOrCreate()
    
    //data example
    //{"impressionID": "100001", "ImpressionTime": "2020-11-01 11:00:00", "CampaignName": "Trendytech"}
    //{"impressionID": "100002", "ImpressionTime": "2020-11-01 11:06:00", "CampaignName": "BigDataBySumit"}
    //{"impressionID": "100003", "ImpressionTime": "2020-11-01 11:02:00", "CampaignName": "Trendytech Insights"}
    //{"impressionID": "100004", "ImpressionTime": "2020-11-01 11:09:00", "CampaignName": "Trendytech"}

    val impressionschema = StructType(List(
    StructField("impressionID",StringType),
    StructField("ImpressionTime",TimestampType),
    StructField("CampaignName",StringType)
    ))
    
    //data example
    //{"clickID": "100001", "ClickTime": "2020-11-01 11:18:00"}
    //{"clickID": "100002", "ClickTime": "2020-11-01 11:18:00"}
    //{"clickID": "100003", "ClickTime": "2020-11-01 11:18:00"}
    //{"clickID": "100004", "ClickTime": "2020-11-01 11:18:00"}
    //{"clickID": "100001", "ClickTime": "2020-11-01 11:18:00"}    
    val clickschema = StructType(List(
    StructField("clickID",StringType),
    StructField("ClickTime",TimestampType)
    ))    
   
     val impressionDf = spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", "8889")
    .load()
    
    val impressionvalueDf = impressionDf.select(from_json(col("value"),impressionschema).alias("value"))
    val impressionrefinedDf = impressionvalueDf.select("value.*").withWatermark("ImpressionTime", "30 minute")
    impressionrefinedDf.printSchema()
    
    
     val clickDf = spark.readStream.format("socket")
    .option("host", "localhost")
    .option("port", "8899")
    .load()
    
    val clickvalueDf = clickDf.select(from_json(col("value"),clickschema).alias("value"))
    val clickrefinedDf = clickvalueDf.select("value.*").withWatermark("ClickTime", "15 minute")
    clickrefinedDf.printSchema()
    
    
    val joinexpr = impressionrefinedDf.col("ImpressionID") === clickrefinedDf.col("clickID")
    //val joinexpr = impressionrefinedDf.col("ImpressionID") === clickrefinedDf.col("clickID")
    //In case of leftOuter join use the below mentioned join expression
    //val joinexpr = expr("ImpressionID == clickID AND ClickTime BETWEEN ImpressionTime AND ImpressionTime + interval 15 minute ")
    val jointype = "inner"
    
    val enricheddf = impressionrefinedDf.join(clickrefinedDf,joinexpr,jointype)
    
    
    //print output to console, or write to the sink
     val joinquery = enricheddf.writeStream
    .format("console")
    .outputMode("append") //if output mode is complete watermark is overridden, so it will not allow us to clean the state store
    //spark allows to use append mode when we are using watermarks on event time
    //append will only print when the window is being discarded, since appennd does not allow updates
    .option("checkpointLocation", "chkpntloc12")
    .trigger(Trigger.ProcessingTime("15 second"))
    .start
    
    
    joinquery.awaitTermination()
    
    
}