import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger



object StreamProcessingStateless extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[2]","wordcount")
    
    //creating spark dstream context
    val ssc = new StreamingContext(sc,Seconds(5))
    
    //lines is a dstream
    val lines =  ssc.socketTextStream("localhost",9998)
    
    //words is a transformed dstream
    val words = lines.flatMap(x=>x.split(" "))
    val pairs = words.map(word=>(word,1))
    val wordCounts = pairs.reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
}