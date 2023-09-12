
import org.apache.spark.sql.SparkSession

object readParquet extends App {

  val spark = SparkSession
    .builder()
    .master("local[3]")
    .appName("SparkFirst")
    .getOrCreate()

  val parqDF1 = spark.read
    .parquet("outPutData/ShowCase_corporate_account_.parquet")
  parqDF1.show()

  val parqDF2 = spark.read
    .parquet("outPutData/ShowCase_corporate_info_.parquet")
  parqDF2.show()

  val parqDF3 = spark.read
    .parquet("outPutData/ShowCase_corporate_payments_.parquet")
  parqDF3.show()

}