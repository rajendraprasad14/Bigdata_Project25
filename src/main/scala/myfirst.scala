import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object myfirst {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("Spark DataFrame Example")
      .master("local[*]") // Use all available cores
      .getOrCreate()
    val df=spark.read
      .format("csv")
      .option("header","true")
      .option("path","D:/Studymaterial/Karthik/details.csv")
      .load()
    df.show()
    // Stop Spark
    spark.stop()
  }
}
