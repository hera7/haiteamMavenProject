package com.haiteam

import org.apache.spark
import org.apache.spark.sql.SparkSession

class Test1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("jyTestEx").
      config("spark.master", "local").
      getOrCreate()


  // 파일설정
//  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
//  var staticUser = "kopo"
//  var staticPw = "kopo"
//  var selloutDb = "kopo_product_volume"

    var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
    var staticUser = "kopo"
    var staticPw = "kopo"
    var selloutDb = "KOPO_CHANNEL_SEASONALITY_NEW"




  // jdbc (java database connectivity)
  val selloutDataFromOrcl = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load

    selloutDataFromOrcl.createOrReplaceTempView("selloutTable")
    selloutDataFromOrcl.show(2)

    spark.sql("select regionid, PRODUCTGROUP, YEARWEEK, VOLUME * 1.2 AS QTY_NEW FROM selloutTable")

}
}


