import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger



object StreamProcessingStatefullFullStream extends App{
      Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[2]","wordcount")
    
    //creating spark dstream context
    val ssc = new StreamingContext(sc,Seconds(5))
    
    //lines is a dstream
    val lines =  ssc.socketTextStream("localhost",9998)
    
    //checkpoint directory to maintain state
    ssc.checkpoint(".")
    //words is a transformed dstream
    val words = lines.flatMap(x=>x.split(" "))
    val pairs = words.map(word=>(word,1))
    
    //below is a stateless transformation
    //val wordCounts = pairs.reduceByKey(_+_)
    def updatefunc(newValues:Seq[Int],previousState:Option[Int]):Option[Int]  = {
        val newCount = previousState.getOrElse(0) + newValues.sum
        Some(newCount)
      }
    
    //below is a statefull transformation
     //the update func  taakes in 2 args as shown above
    val wordCounts = pairs.updateStateByKey(updatefunc)
    
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
}