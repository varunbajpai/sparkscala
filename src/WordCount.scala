import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object WordCount extends App {
Logger.getLogger("org").setLevel(Level.ERROR)
val sc = new SparkContext("local[*]","wordcount")
val input = sc.textFile("/Users/vbajpai/Downloads/trendy tech bigdata/temp/test.txt")
val lowerwords = input.flatMap(_.split(" ")).map(_.toLowerCase())
val wordmap = lowerwords.map((_,1))
val wordmap2 = wordmap.reduceByKey(_+_)

val wordmaprev = wordmap2.map(x=> (x._2,x._1))
val finalcount = wordmaprev.sortByKey(false).map(x=> (x._2,x._1))
finalcount.collect.foreach(println)

scala.io.StdIn.readLine()
}