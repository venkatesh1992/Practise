import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.sql.functions._
object Delt extends App{
  //val conf=new SparkConf().setMaster("local[*]").setAppName("delt")
  val spark = SparkSession.builder.appName("Delt").getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val sc=spark.sparkContext
  import spark.implicits._

    //reading the file
  val df=spark.read.option("header",true).csv("/user/venkat/data.csv")
  //converting to parquet
  val df1=df.write.parquet("/user/venkat/data_parquet")
  //reading parquet file again
  val df2=spark.read.parquet("/user/venkat/data_parquet")
  //conveting to required format
  val df3=df2.withColumn("tmp", split($"Values", "\\;")).select(col("Country"),
    $"tmp".getItem(0).as("col1").cast("int"),
    $"tmp".getItem(1).as("col2").cast("int"),
    $"tmp".getItem(2).as("col3").cast("int"),
    $"tmp".getItem(3).as("col4").cast("int"))
  //doing aggregation
  val df4=df3.groupBy("Country").agg(sum("col1") as "col1",sum("col2") as "col2",sum("col3") as "col3",sum("col4") as "col4")
  //converting to final required format
  val df5=df4.select($"Country",concat_ws(";",$"col1",$"col2",$"col3",$"col4") as "Values")
  //saving the result to parquet again
  df5.write.parquet("user/venkat/final_result")

}
