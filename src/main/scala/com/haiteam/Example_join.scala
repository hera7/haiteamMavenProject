package com.haiteam
import org.apache.spark.sql.SparkSession
object Example_join {
  val spark = SparkSession.builder().appName("...").config("spark.master", "local").getOrCreate()


var mainFile = "kopo_channel_seasonality_ex.csv"
var subFile = "kopo_product_mst.csv"

var dataPath = "c:/spark/bin/data/"
// 상대경로 입력
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