
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}

object ProjectClientsBills extends App {

  // Create SparkSession
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


  //ВИТРИНА _corporate_payments_ Строится по каждому уникальному счету (AccountDB и AccountCR) из таблицы Operation.
  // Ключ партиции CutoffDt

  val dfRateCurrent = dfRate.groupBy("Currency")
    .agg(max("RateDate").alias("RateDate"))
    .join(dfRate, Seq("Currency", "RateDate"))
    .select("Currency", "RateDate", "Rate")

  dfRateCurrent.createOrReplaceTempView("RateCurrent")
  dfAccount.createOrReplaceTempView("Accounts")
  dfClient.createOrReplaceTempView("Clients")
  dfOperation.createOrReplaceTempView("CorporatePayments")

  // PaymentAmt Сумма операций по счету, где счет клиента указан в дебете проводки
  val dfAccountPaymentsCurrency = spark.sql("SELECT AccountDB, DateOP AS CutoffDt, " +
    "ROUND(SUM(Amount * RateCurrent.Rate), 2) AS PaymentAmt " +
    "FROM CorporatePayments " +
    "JOIN RateCurrent ON (RateCurrent.Currency = CorporatePayments.Currency AND RateCurrent.RateDate <= CorporatePayments.DateOP)" +
    "GROUP BY CorporatePayments.AccountDB, CorporatePayments.DateOP, CorporatePayments.Currency, RateCurrent.Rate")

  val dfAccountPayments = dfAccountPaymentsCurrency
    .groupBy("AccountDB", "CutoffDt")
    .agg(round(sum("PaymentAmt"), 2).as("PaymentAmt"))
    .sort("AccountDB")

  //  EnrollementAmt - Сумма операций по счету, где счет клиента указан в кредите проводки
  val dfAccountEnrollementsCurrency = spark.sql("SELECT AccountCR, DateOp AS CutoffDt, " +
    "ROUND(SUM(Amount * RateCurrent.Rate), 2) AS EnrollementAmt FROM CorporatePayments " +
    "JOIN RateCurrent ON (RateCurrent.Currency = CorporatePayments.Currency AND RateCurrent.RateDate <= CorporatePayments.DateOp) " +
    "GROUP BY CorporatePayments.AccountCR, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")

  val dfAccountEnrollement = dfAccountEnrollementsCurrency
    .groupBy("AccountCR", "CutoffDt")
    .agg(round(sum("EnrollementAmt"), 2).as("EnrollementAmt"))

  //  TaxAmt - Сумма операций, где счет клиента указан в дебете, и счет кредита 40702
  val dfTaxAmtCurrency = spark.sql("SELECT AccountDB, DateOp AS CutoffDt, " +
    "ROUND(SUM(Amount * RateCurrent.Rate), 2) AS TaxAmt FROM CorporatePayments " +
    " JOIN RateCurrent ON (RateCurrent.Currency = CorporatePayments.Currency AND RateCurrent.RateDate <= CorporatePayments.DateOp) " +
    "WHERE AccountCR = 40702 " +
    "GROUP BY CorporatePayments.AccountDB, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")

  val dfTaxAmt = dfTaxAmtCurrency
    .groupBy("AccountDB", "CutoffDt")
    .agg(round(sum("TaxAmt"), 2).as("TaxAmt"))


  //  ClearAmt - Сумма операций, где счет клиента указан в кредите, и счет дебета 40802
  val dfClearAmtCurrency = spark.sql("SELECT AccountCR, DateOp AS CutoffDt, " +
    "ROUND(SUM(Amount * RateCurrent.Rate), 2) AS ClearAmt FROM CorporatePayments " +
    " JOIN RateCurrent ON (RateCurrent.Currency = CorporatePayments.Currency AND RateCurrent.RateDate <= CorporatePayments.DateOp) " +
    "WHERE AccountDB = 40802 " +
    "GROUP BY CorporatePayments.AccountCR, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")

  val dfClearAmt = dfClearAmtCurrency
    .groupBy("AccountCR", "CutoffDt")
    .agg(round(sum("ClearAmt"), 2).as("ClearAmt"))


  //  CarsAmt - Сумма операций, где счет клиента указан в дебете проводки и назначение платежа не содержит слов по маскам Списка 1
  val dfCarsAmtCurrency = spark.sql("SELECT AccountDB, DateOp AS CutoffDt, " +
    "ROUND(SUM(Amount * RateCurrent.Rate), 2) AS CarsAmt FROM CorporatePayments " +
    "JOIN RateCurrent ON (RateCurrent.Currency = CorporatePayments.Currency AND RateCurrent.RateDate <= CorporatePayments.DateOp) " +
    "WHERE (Comment NOT LIKE '%а/м%' AND Comment NOT LIKE '%а\\м%' AND Comment NOT LIKE '%автомобиль %' AND Comment NOT LIKE '%автомобили %' AND Comment NOT LIKE '%транспорт%' AND Comment NOT LIKE '%трансп%средс%'" +
    " AND Comment NOT LIKE'%легков%' AND Comment NOT LIKE '%тягач%' AND Comment NOT LIKE '%вин%' AND Comment NOT LIKE '%vin%' AND Comment NOT LIKE '%viн:%' AND Comment NOT LIKE '%fоrd%' AND Comment NOT LIKE '%форд%' AND Comment NOT LIKE '%кiа%' AND Comment NOT LIKE '%кия%' AND Comment NOT LIKE '%киа%%мiтsuвisнi%' " +
    " AND Comment NOT LIKE '%мицубиси%' AND Comment NOT LIKE '%нissан%' AND Comment NOT LIKE '%ниссан%' AND Comment NOT LIKE '%sсанiа%' AND Comment NOT LIKE '%вмw%' AND Comment NOT LIKE '%бмв%' AND Comment NOT LIKE '%аudi%' AND Comment NOT LIKE '%ауди%' AND Comment NOT LIKE '%jеер%' AND Comment NOT LIKE '%джип%' " +
    " AND Comment NOT LIKE '%vоlvо%' AND Comment NOT LIKE '%вольво%' AND Comment NOT LIKE '%тоyота%' AND Comment NOT LIKE '%тойота%' AND Comment NOT LIKE '%тоиота%' AND Comment NOT LIKE '%нyuнdаi%' AND Comment NOT LIKE '%хендай%' AND Comment NOT LIKE '%rенаulт%' AND Comment NOT LIKE '%рено%' " +
    " AND Comment NOT LIKE '%реugеот%' AND Comment NOT LIKE '%пежо%' AND Comment NOT LIKE '%lаdа%' AND Comment NOT LIKE '%лада%' AND Comment NOT LIKE '%dатsuн%' AND Comment NOT LIKE '%додж%' AND Comment NOT LIKE '%меrсеdеs%' AND Comment NOT LIKE '%мерседес%' AND Comment NOT LIKE '%vоlкswаgен%' " +
    " AND Comment NOT LIKE '%фольксваген%' AND Comment NOT LIKE '%sкоdа%' AND Comment NOT LIKE '%шкода%' AND Comment NOT LIKE '%самосвал%' AND Comment NOT LIKE '%rover%' AND Comment NOT LIKE '%ровер%') " +
    " GROUP BY CorporatePayments.AccountDB, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")

  val dfCarsAmt = dfCarsAmtCurrency
    .groupBy("AccountDB", "CutoffDt")
    .agg(round(sum("CarsAmt"), 2).as("CarsAmt"))


  //  FoodAmt - Сумма операций, где счет клиента указан в кредите проводки и назначение платежа содержит слова по Маскам Списка 2
  val dfFoodAmtCurrency = spark.sql("SELECT AccountCR, DateOp AS CutoffDt," +
    "ROUND(SUM(Amount * RateCurrent.Rate), 2) AS FoodAmt FROM CorporatePayments " +
    "JOIN RateCurrent ON (RateCurrent.Currency = CorporatePayments.Currency AND RateCurrent.RateDate <= CorporatePayments.DateOp) " +
    "WHERE (Comment LIKE '%сою%' OR Comment LIKE '%соя%' OR Comment LIKE '%зерно%' OR Comment LIKE '%кукуруз%' OR Comment LIKE '%масло%' OR Comment LIKE '%молок%' OR Comment LIKE '%молоч%' OR Comment LIKE '%мясн%' " +
    " OR Comment LIKE '%мясо%' OR Comment LIKE '%овощ%' OR Comment LIKE '%подсолн%' OR Comment LIKE '%пшениц%' OR Comment LIKE '%рис%' OR Comment LIKE '%с/х%прод%' OR Comment LIKE '%с/х%товар%' OR Comment LIKE '%с\\х%прод%' OR Comment LIKE '%с\\х%товар%' " +
    " OR Comment LIKE '%сахар%' OR Comment LIKE '%сельск%прод%' OR Comment LIKE '%сельск%товар%' OR Comment LIKE '%сельхоз%прод%' OR Comment LIKE '%сельхоз%товар%' OR Comment LIKE '%семен%' OR Comment LIKE '%семечк%' " +
    " OR Comment LIKE '%сено%%' OR Comment LIKE '%%соев%' OR Comment LIKE '%фрукт%' OR Comment LIKE '%яиц%' OR Comment LIKE '%ячмен%' OR Comment LIKE '%картоф%' OR Comment LIKE '%томат%' OR Comment LIKE '%говя%' OR Comment LIKE '%свин%' OR Comment LIKE '%курин%' " +
    " OR Comment LIKE '%куриц%' OR Comment LIKE '%рыб%' OR Comment LIKE '%алко%' OR Comment LIKE '%чаи%' OR Comment LIKE '%кофе%' OR Comment LIKE '%чипс%' OR Comment LIKE '%напит%' OR Comment LIKE '%бакале%' OR Comment LIKE '%конфет%' OR Comment LIKE '%колбас%' " +
    " OR Comment LIKE '%морож%' OR Comment LIKE '%с/м%' OR Comment LIKE '%с\\м%' OR Comment LIKE '%консерв%' OR Comment LIKE '%пищев%' OR Comment LIKE '%питан%' OR Comment LIKE '%сыр%' OR Comment LIKE '%макарон%' OR Comment LIKE '%лосос%' OR Comment LIKE '%треск%' " +
    " OR Comment LIKE '%саир%' OR Comment LIKE '%филе%' OR Comment LIKE '%хек%' OR Comment LIKE '%хлеб%' OR Comment LIKE '%какао%' OR Comment LIKE '%кондитер%' OR Comment LIKE '%пиво%' OR Comment LIKE '%ликер%') " +
    "GROUP BY CorporatePayments.AccountCR, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")

  val dfFoodAmt = dfFoodAmtCurrency
    .groupBy("AccountCR", "CutoffDt")
    .agg(round(sum("FoodAmt"), 2).as("FoodAmt"))


  //  FLAmt - Сумма операций с физлицами. Счет клиента указан в дебете проводки, а клиент в кредите проводки — ФЛ
  val dfFLAmtCurrency = spark.sql("SELECT AccountDB, DateOp AS CutoffDt, " +
    "ROUND(SUM(Amount * RateCurrent.Rate), 2) AS FLAmt, RateCurrent.Rate, CorporatePayments.Currency " +
    "FROM CorporatePayments " +
    "JOIN RateCurrent ON (RateCurrent.Currency = CorporatePayments.Currency AND RateCurrent.RateDate <= CorporatePayments.DateOp) " +
    "JOIN Accounts ON AccountCR = AccountID " +
    "JOIN Clients on Accounts.ClientId = Clients.ClientId " +
    "WHERE Clients.Type = 'Ф' " +
    "GROUP BY CorporatePayments.AccountDB, CorporatePayments.DateOp, CorporatePayments.Currency, RateCurrent.Rate")

  val dfFLAmt = dfFLAmtCurrency
    .groupBy("AccountDB", "CutoffDt")
    .agg(round(sum("FLAmt"), 2).as("FLAmt"))
    .sort("AccountDB")


  val dfCorporatePaymentsDB = dfAccountPayments
    .join(dfTaxAmt, Seq("AccountDB", "CutoffDt"), "outer")
    .join(dfCarsAmt, Seq("AccountDB", "CutoffDt"), "outer")
    .join(dfFLAmt, Seq("AccountDB", "CutoffDt"), "outer")

  val dfCorporatePaymentsCR = dfAccountEnrollement
    .join(dfClearAmt, Seq("AccountCR", "CutoffDt"), "outer")
    .join(dfFoodAmt, Seq("AccountCR", "CutoffDt"), "outer")

  dfCorporatePaymentsDB.createOrReplaceTempView("CorporatePaymentsDB")

  val dfCorporatePaymentsDBWithClient = spark.sql("SELECT CorporatePaymentsDB.AccountDB AS AccountId, " +
    "Accounts.ClientId, CorporatePaymentsDB.CutoffDt, CorporatePaymentsDB.PaymentAmt, CorporatePaymentsDB.TaxAmt, " +
    "CorporatePaymentsDB.CarsAmt, CorporatePaymentsDB.FLAmt " +
    "FROM CorporatePaymentsDB " +
    "JOIN Accounts ON CorporatePaymentsDB.AccountDB = Accounts.AccountId")

  dfCorporatePaymentsCR.createOrReplaceTempView("CorporatePaymentsCR")

  val dfCorporatePaymentsCRWithClient = spark.sql("SELECT CorporatePaymentsCR.AccountCR AS AccountId, " +
    "Accounts.ClientId, CorporatePaymentsCR.CutoffDt, CorporatePaymentsCR.EnrollementAmt, CorporatePaymentsCR.ClearAmt, " +
    "CorporatePaymentsCR.FoodAmt " +
    "FROM CorporatePaymentsCR " +
    "JOIN Accounts ON CorporatePaymentsCR.AccountCR = Accounts.AccountId")

  val dfCorporatePaymentsShowCaseDisOrder = dfCorporatePaymentsDBWithClient
    .join(dfCorporatePaymentsCRWithClient, Seq("AccountId", "ClientId", "CutoffDt"), "outer")

  dfCorporatePaymentsShowCaseDisOrder.createOrReplaceTempView("CorporatePaymentsShowCase")

  val dfCorporatePaymentsShowCase = spark.sql("SELECT AccountId, ClientId, PaymentAmt, EnrollementAmt, " +
    "TaxAmt, ClearAmt, CarsAmt, FoodAmt, FLAmt, CutoffDt " +
    "FROM CorporatePaymentsShowCase " +
    "ORDER BY AccountId, CutoffDt")

  dfCorporatePaymentsShowCase.write
    .mode("overwrite")
    .partitionBy("CutoffDt")
    .parquet("outPutData/ShowCase_corporate_payments_.parquet")


  //ВИТРИНА _corporate_account_. Строится по каждому уникальному счету из таблицы Operation на заданную дату расчета.
  // Ключ партиции CutoffDt

  val dfCorporateAccount = spark.sql("SELECT CorporatePaymentsShowCase.AccountId, Accounts.AccountNum, " +
    "Accounts.DateOpen, CorporatePaymentsShowCase.ClientId, Clients.ClientName, " +
    "(CASE WHEN PaymentAmt IS NULL THEN 0 ELSE PaymentAmt END " +
    "+ CASE WHEN EnrollementAmt IS NULL THEN 0 ELSE EnrollementAmt END) AS TotalAmt, CutoffDt " +
    "FROM CorporatePaymentsShowCase " +
    "JOIN Clients ON CorporatePaymentsShowCase.ClientId = Clients.ClientId " +
    "JOIN Accounts ON CorporatePaymentsShowCase.AccountId = Accounts.AccountId " +
    "ORDER BY AccountId, CutoffDt")

  dfCorporateAccount.createOrReplaceTempView("CorporateAccount")
  dfCorporateAccount.write
    .mode("overwrite")
    .partitionBy("CutoffDt")
    .parquet("outPutData/ShowCase_corporate_account_.parquet")


  //  ВИТРИНА _corporate_info_. Строится по каждому уникальному клиенту из таблицы Operation. Ключ партиции CutoffDt

  val dfCorporateInfo = spark.sql("SELECT CorporateAccount.ClientId, CorporateAccount.ClientName, Type, Form, " +
    "RegisterDate, sum(TotalAmt) AS TotalAmt, CutoffDt " +
    "FROM CorporateAccount " +
    "JOIN Clients ON CorporateAccount.ClientId = Clients.ClientId " +
    "GROUP BY CorporateAccount.ClientId, CorporateAccount.ClientName, Type, Form, RegisterDate, CutoffDt " +
    "ORDER By CorporateAccount.ClientId, CutoffDt")

  dfCorporateInfo.write
    .mode("overwrite")
    .partitionBy("CutoffDt")
    .parquet("outPutData/ShowCase_corporate_info_.parquet")

  //Вывод схем витрин
  dfCorporatePaymentsShowCase.printSchema()
  dfCorporateAccount.printSchema()
  dfCorporateInfo.printSchema()

}
