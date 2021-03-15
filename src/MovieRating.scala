
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
 
object MovieRating extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  val input = sc.textFile("/Users/vbajpai/Downloads/trendy tech bigdata/temp/moviedata-201008-180523.data")
  val col = input.map(_.split("\t")(2)).map((_,1))
  val ans = col.reduceByKey((x,y)  => x+y)
  val results = ans.collect
  results.foreach(println)
  scala.io.StdIn.readLine()
}