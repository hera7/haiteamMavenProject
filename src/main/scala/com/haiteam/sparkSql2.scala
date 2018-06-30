package com.haiteam

import org.apache.spark.sql.SparkSession

object sparkSql2 {
  val spark = SparkSession.builder().appName("hkProject").
    config("spark.master", "local").
    getOrCreate()

  // 접속정보 설정
  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb1 = "kopo_channel_seasonality_new"
  var selloutDb2 = "kopo_region_mst"

  // jdbc (java database connectivity) 연결
  val mainData1 = spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable" -> selloutDb1,"user" -> staticUser, "password" -> staticPw)).load
  val subData1= spark.read.format("jdbc").options(Map("url" -> staticUrl,"dbtable" -> selloutDb2,"user" -> staticUser, "password" -> staticPw)).load

  // 메모리 테이블 생성
  mainData1.createOrReplaceTempView("maindata1")
  subData1.createOrReplaceTempView("subdata1")
  mainData1.show()
  subData1.show()
}
