package com.haiteam

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, DoubleType, StructField, StructType}
import org.apache.spark.sql.Row

class RddRaw {




  object ExSeasonality {
    def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder().appName("mavenProject").
        config("spark.master", "local").
        getOrCreate()


      //oracle connection
      var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

      var staticUser = "kopo"
      var staticPw = "kopo"
      var selloutDb = "kopo_channel_seasonality_new"
      var masterDb = "kopo_product_mst"

      val selloutDf = spark.read.format("jdbc").
        option("encoding", "UTF-8").
        options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
          "user" -> staticUser,
          "password" -> staticPw)).load

      val mstDf = spark.read.format("jdbc").option("encoding", "UTF-8").
        options(Map("url" -> staticUrl, "dbtable" -> masterDb, "user" -> staticUser, "password" -> staticPw)).load

      selloutDf.createOrReplaceTempView("selloutTable")

      mstDf.createOrReplaceTempView("mstTable")

      var rawData = spark.sql("select concat(a.regionid,'_', a.product) as keycol, " +
        "a.product, " +
        "a.regionid as accountid," +
        "a.yearweek, " +
        "cast(a.qty as String) as qty, " +
        "'test' as productname from selloutTable a ")

      rawData.show(2)

      var rawDataColumns = rawData.columns

      var keyNo = rawDataColumns.indexOf("keycol")
      var accountidNo = rawDataColumns.indexOf("accountid")
      var productNo = rawDataColumns.indexOf("product")
      var yearweekNo = rawDataColumns.indexOf("yearweek")
      var qtyNo = rawDataColumns.indexOf("qty")
      var productnameNo = rawDataColumns.indexOf("productname")

      var rawRdd = rawData.rdd

      var rawExRdd = rawRdd.filter(x=>{
        var checkValid = true

        var weekValue = x.getString(yearweekNo).substring(4).toInt

        // 비교한후 주차정보가 53 이상인 경우 레코드 삭제
        if( weekValue >= 53){
          checkValid = false
        }
        checkValid
      })
      // 분석대상 제품군 등록
      var productArray = Array("PRODUCT1","PRODUCT2")
      //세트 타입으로 변환
      var productSet = productArray.toSet

      var resultRdd = rawExRdd.filter(x=>{
        var checkValid = true

        //데이터 특정 행의 product 컬럼인덱스를 활용하여 데이터 대입
        var productInfo = x.getString(productNo);

        if(productSet.contains(productInfo)){
          checkValid = true
        }

        checkValid
      })

      val finalResultDf = spark.createDataFrame(resultRdd,
        StructType(
          Seq(
            StructField("KEY", StringType),
            StructField("REGIONID", StringType),
            StructField("PRODUCT", StringType),
            StructField("YEARWEEK", StringType),
            StructField("VOLUME", StringType),
            StructField("PRODUCT_NAME", StringType))))

      // key, account, product, yearweek, qty, productname
      var mapRdd = resultRdd.map(x=>{
        var qty = x.getString(qtyNo).toDouble
        var maxValue = 700000
        if(qty > 700000){qty = 700000}
        Row( x.getString(keyNo),
          x.getString(yearweekNo),
          qty)//x.getString(qtyNo)
      })

      var MAXVALUE = 700000

      var maxRdd = filterRdd.map(x=>{first})





    }
  }

}
