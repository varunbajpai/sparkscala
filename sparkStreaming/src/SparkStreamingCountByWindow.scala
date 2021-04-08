import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger

//count number of lines cominng in as input in spark stream
object SparkStreamingCountByWindow extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","wordcount")
    val ssc = new StreamingContext(sc,Seconds(2))
    val lines =  ssc.socketTextStream("localhost",9998)
    ssc.checkpoint(".")
    

    def windowDuration()= {
      Seconds(10)
    }
    def  slideDuration() = {
      Seconds(2)
    }
    //using functions as seperate logic
    val words = lines.countByWindow(windowDuration, slideDuration)

    words.print()
    ssc.start()
    ssc.awaitTermination()
}