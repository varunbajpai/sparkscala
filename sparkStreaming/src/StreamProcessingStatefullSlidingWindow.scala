import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.log4j.Level
import org.apache.log4j.Logger

object StreamProcessingStatefullSlidingWindow extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","wordcount")
    val ssc = new StreamingContext(sc,Seconds(2))
    val lines =  ssc.socketTextStream("localhost",9998)
    ssc.checkpoint(".")
    
    def reduceFunc(x:Int,y:Int)= {
      x+y
    }
    def invReduceFunc(x:Int,y:Int)= {
      x-y
    } 
    def windowDuration()= {
      Seconds(10)
    }
    def  slideDuration() = {
      Seconds(2)
    }
    //using functions as seperate logic
    val words = lines.flatMap(x=>x.split(" "))
               .map(word=>(word,1))
               .reduceByKeyAndWindow(reduceFunc(_,_), invReduceFunc(_,_), windowDuration, slideDuration)
               .filter(x => x._2>0) //it willl show up if value > 0 else it will not show up
    words.print()
    ssc.start()
    ssc.awaitTermination()
}