package com.haiteam

import org.apache.spark.sql.SparkSession

object Example_join {
  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()



  var staticUrl = "jdbc:mysql://192.168.110.111:1521/orcl"
  var staticUser  = "kopo"
  var staticPw = "kopo"
  var mainFile = "kopo_channel_seasonality_new"
  var subFile = "kopo_region_mst"

 // jdbc (java database connectivity) 연결
 val selloutData1= spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable" -> mainFile,"user" -> staticUser, "password" -> staticPw)).load
  val selloutData2= spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable" -> subFile,"user" -> staticUser, "password" -> staticPw)).load



      // 메모리 테이블 생성
  selloutData1.createOrReplaceTempView("selloutTable")
  selloutData2.createOrReplaceTempView("selloutTable2")
  selloutData1.show()
  selloutData2.show()


  var dataPath = "/.data/"

  var mainData = spark.read.format("csv").
    option("header", "true").load(dataPath + mainFile)
  var subData = spark.read.format("csv").
    option("header", "true").load(dataPath + subFile)

  mainData.createOrReplaceTempView("mainTable")
  subData.createOrReplaceTempView("subTable")

  var leftJoinData = spark.
    sql("select a.regionid, " +
      "a.productgroup," +
      " b.productname," +
      " a.yearweek, " +
      "a.qty "+ "from mainTable a "+"left join subTable b "+ "on a.productgroup = b.productid")

}
