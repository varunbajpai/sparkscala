import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.Source

object CoalesceVsRepartition extends App{
//    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","wordcount")
    val baseRdd = sc.textFile("/Users/vbajpai/Downloads/trendy tech bigdata/temp/bigLogNew.txt")
    
    //Coalesce happens within the same  machine it collects all the partitions in the same machine 
    //and combines them, hence it avoids reshuffling(in some extreme cases some shuffling might still happen), repartition does reshuffling
    print(baseRdd.getNumPartitions)
    //val rdd2 = baseRdd.repartition(10)//this will create 2 stages since shuffling is involved
    val rdd2 = baseRdd.coalesce(10)//extra stage will not be created in this case since same machines rdd commbinne, but the partitions can be of unequal sizes
    //repartition involves full shuffling which is time connsuming but resulting partitions will be of same size
    
   rdd2.count
    
  
    scala.io.StdIn.readLine()
  
}