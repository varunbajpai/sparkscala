
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level

object FriendsData extends App {
  
  def parseline(line: String) = {
    val linesplit = line.split("::")
    (linesplit(2).toInt, linesplit(3).toInt)
  }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  val input = sc.textFile("/Users/vbajpai/Downloads/trendy tech bigdata/temp/friendsdata.csv")
  val mappedinp = input.map(parseline)
  val mappedfinal = mappedinp.map(x=>(x._1,(x._2,1)))
  val ans = mappedfinal.reduceByKey((x,y) =>(x._1+y._1, x._2+y._2) )
  val totalbyage = ans.map(x => (x._1,x._2._1/x._2._2)).sortBy(x=> x._2)
  
  totalbyage.collect.foreach(println)
}