import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.SparkSession

object checkData extends App {

  val spark = SparkSession
    .builder()
    .master("local[3]")
    .appName("SparkFirst")
    .getOrCreate()

  val schemaClient = StructType(Array(
    StructField("ClientId", IntegerType, nullable = true),
    StructField("ClientName", StringType, nullable = true),
    StructField("Type", StringType, nullable = true),
    StructField("Form", StringType, nullable = true),
    StructField("RegisterDate", DateType, nullable = true)))

  val schemaAccount = StructType(Array(
    StructField("AccountId", IntegerType, nullable = true),
    StructField("AccountNum", StringType, nullable = true),
    StructField("ClientId", IntegerType, nullable = true),
    StructField("DateOpen", DateType, nullable = true)))

  val schemaOperation = StructType(Array(
    StructField("AccountDb", IntegerType, nullable = true),
    StructField("AccountCR", IntegerType, nullable = true),
    StructField("DateOp", DateType, nullable = true),
    StructField("Amount", StringType, nullable = true),
    StructField("Currency", StringType, nullable = true),
    StructField("Comment", StringType, nullable = true)))

  val schemaCurrency = StructType(Array(
    StructField("Currency", StringType, nullable = true),
    StructField("Rate", StringType, nullable = true),
    StructField("RateDate", DateType, nullable = true)))

  val dfClient = spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .schema(schemaClient)
    .load("inputData/Clients.csv")

  val dfAccount = spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "utf-8")
    .schema(schemaAccount)
    .load("inputData/Account.csv")

  val dfOperation = spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "utf-8")
    .schema(schemaOperation)
    .load("inputData/Operation.csv")
    .withColumn("Amount", regexp_replace(col("Amount"), ",", ".").cast(FloatType))

  val dfRate = spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "utf-8")
    .schema(schemaCurrency)
    .load("inputData/Rate.csv")
    .withColumn("Rate", regexp_replace(col("Rate"), ",", ".").cast(FloatType))

  val checkDuplicateClients = dfClient.groupBy("ClientId").count().filter("count > 1").count()

  val checkDuplicateAccounts = dfAccount.groupBy("AccountId").count().filter("count > 1").count()

  val checkEmptyValuesOperations = dfOperation.filter("AccountDB IS NULL OR AccountCR IS NULL OR DateOp " +
    "IS NULL OR Amount IS NULL OR Currency IS NULL OR Comment IS NULL").count()

  val checkNullValuesOperations = dfOperation.filter("Amount = 0").count()

  val checkEmptyValuesClients = dfClient.filter("ClientId IS NULL OR ClientName IS NULL OR Type " +
    "IS NULL OR Form IS NULL OR RegisterDate IS NULL").count()

  println("Number of duplicates in the Client table: " + checkDuplicateClients)
  println("Number of duplicates in the Account table: " + checkDuplicateAccounts)
  println("Number of duplicates in the Account table: " + checkDuplicateAccounts)
  println("Number of empty values in the Operation table: " + checkEmptyValuesOperations)
  println("Number of values with zero data in the Operation table: " + checkNullValuesOperations)
  println("Number of empty values in the Client table: " + checkEmptyValuesClients)

}
