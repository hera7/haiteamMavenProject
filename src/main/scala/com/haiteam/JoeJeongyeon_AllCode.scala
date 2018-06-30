package com.haiteam

import org.apache.spark
import org.apache.spark.sql.SparkSession


class JoeJeongyeon_AllCode {


  var staticUrl = "jdbc:oracle:thin:@192.168.110.112:1521/orcl"
  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "KOPO_CHANNEL_SEASONALITY_NEW"


  val selloutDataFromOrcl = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> selloutDb, "user" -> staticUser, "password" -> staticPw)).load


//  selloutDataFromOrcl.createOrReplaceTempView("selloutTable")
//  selloutDataFromOrcl.show(2)


//  2.
//  var NewData = spark.sql("select REGIONID, PRODUCT, YEARWEEK, cast(QTY as Double) , cast(QTY * 1.2 as Double) AS QTY_NEW FROM selloutTable")
//
//  NewData.show(2)


//  4.

  var rawDataColumns = NewData.columns

  var regionNo = rawDataColumns.indexOf("REGIONID")
  var productNo = rawDataColumns.indexOf("PRODUCT")
  var yearweekNo = rawDataColumns.indexOf("YEARWEEK")
  var qtyNo = rawDataColumns.indexOf("QTY")
  var newqtyNo = rawDataColumns.indexOf("QTY_NEW")

  var RawData = NewData.rdd

  var filteredRdd = RawData.filter(x=>{

    var checkValid = true
    var YEARWEEK = x.getString(yearweekNo).substring(0,4).toInt
    if( YEARWEEK < 2016){
      checkValid = false
    }
    var weekValue = x.getString(yearweekNo).substring(4).toInt
    if( weekValue == 52){
      checkValid = false
    }
    checkValid
  })

  var filteredRdd2 = filteredRdd.filter(x=>{
    var checkValid = false
    if( (x.getString(productNo) == "PRODUCT1") || (x.getString(productNo) == "PRODUCT2")
    ){
      checkValid = true
    }
    checkValid
  })

//  6.
  import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

  val finalResultDf = spark.createDataFrame(filteredRdd2,
    StructType(
      Seq(
        StructField("regionid", StringType),
        StructField("product", StringType),
        StructField("yearweek", StringType),
        StructField("qty", DoubleType),
        StructField("qty_new", DoubleType))))

  var outputUrl = "jdbc:postgresql://192.168.110.111:5432/kopo"

  var outputUser = "kopo"
  var outputPw = "kopo"


  val prop = new java.util.Properties
  prop.setProperty("driver", "org.postgresql.Driver")
  prop.setProperty("user", "kopo")
  prop.setProperty("password", "kopo")
  val table = "kopo_st_middle_joejy"

  //append
  finalResultDf.write.mode("overwrite").jdbc(outputUrl, table, prop)


}
