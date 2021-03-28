//import org.apache.spark.sql.SparkSession
//import org.apache.spark.SparkConf
//import org.apache.log4j.Logger
//import org.apache.log4j.Level
//import org.apache.spark.sql.Dataset
//import org.apache.spark.sql.Row
//import java.sql.Timestamp
//import org.apache.spark.sql.types.LongType
//import org.apache.spark.sql.types.IntegerType
//import org.apache.spark.sql.types.StringType
//import org.apache.spark.sql.types.TimestampType
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.types.StructField
//import org.apache.spark.sql.SaveMode
//
//
object rddToDatasetUnstructuredData extends App{
  
}
//  val myregex = """^(\S+) (\S+) (\S+) (\S+)\,(\S+)""".r
//  
//  case class Orders(order_id:Int,customer_id:Int,order_status:String)
//  
//   def parser(line: String) = {
//   line match{
//     case myregex(order_id,date,customer_id,order_status) => {
//       Orders(order_id.toInt,customer_id.toInt,order_status)
//     }
//   }
// } 
//  
// val sparkConf = new SparkConf()
//  .set("spark.app.name", "My Application 1")
//  .set("spark.master","local[2]")
//  val spark = SparkSession.builder().config(sparkConf).getOrCreate()
//  
//  val lines = spark.sparkContext.textFile("/Users/vbajpai/Downloads/trendy tech bigdata/temp/orders_new")
//  
//  import spark.implicits._
//  lines.map(parser).toDs()
//
//  
//}