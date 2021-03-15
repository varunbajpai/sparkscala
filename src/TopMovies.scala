import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object TopMovies extends App{
    
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","wordcount")
  
  val ratingsRdd = sc.textFile("/Users/vbajpai/Downloads/trendy tech bigdata/temp/ratings.dat")
  val moviesRdd = sc.textFile("/Users/vbajpai/Downloads/trendy tech bigdata/temp/movies.dat")
  
  val moviesRddmapped = moviesRdd.map(x=>{
    val fields = x.split("::")
    (fields(0),fields(1))
  })
  val ratingsRddMapped = ratingsRdd.map(x=> {
    val fields = x.split("::")
    (fields(1),fields(2))
    })
    
  val newmapped = ratingsRddMapped.mapValues(x => (x.toFloat,1.0 )).
                  reduceByKey((x,y) => (x._1 + y._1,x._2+y._2)).
                  filter(x=>x._2._2 > 100).
                  map(x=>(x._1,x._2._1/x._2._2)).
                  sortBy(x=>x._2)
                  
                  
  moviesRddmapped.join(newmapped).map(x=>(x._1,x._2)).sortBy(x=>x._2._2).collect.foreach(println)
    
  scala.io.StdIn.readLine()
  
}