package com.haiteam

import org.apache.spark
import org.apache.spark.sql.Row

object seasonality_final {

  var MAXVALUE = 700000

  /////////////////////////////

  // oracle connection
  var staticUrl = "jdbc:oracle:thin:@192.168.110.111:1521/orcl"

  var staticUser = "kopo"
  var staticPw = "kopo"
  var selloutDb = "kopo_channel_seasonality_new"
  var productNameDb = "kopo_product_mst"

  val selloutDf = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> selloutDb,
      "user" -> staticUser,
      "password" -> staticPw)).load

  val productMasterDf = spark.read.format("jdbc").
    options(Map("url" -> staticUrl, "dbtable" -> productNameDb,
      "user" -> staticUser,
      "password" -> staticPw)).load

  selloutDf.createOrReplaceTempView("selloutTable")
  productMasterDf.createOrReplaceTempView("mstTable")

  var rawData = spark.sql("select " +
    "concat(a.regionid,'_',a.product) as keycol, " +
    "a.regionid as accountid, " +
    "a.product, " +
    "a.yearweek, " +
    "cast(a.qty as double) as qty, " +
    "b.product_name " +
    "from selloutTable a " +
    "left join mstTable b " +
    "on a.product = b.product_id")

  rawData.show(2)

  var rawDataColumns = rawData.columns
  var keyNo = rawDataColumns.indexOf("keycol")
  var accountidNo = rawDataColumns.indexOf("accountid")
  var productNo = rawDataColumns.indexOf("product")
  var yearweekNo = rawDataColumns.indexOf("yearweek")
  var qtyNo = rawDataColumns.indexOf("qty")
  var productnameNo = rawDataColumns.indexOf("product_name")

  // (kecol, accountid, product, yearweek, qty, product_name)
  var rawRdd = rawData.rdd

  var filteredRdd = rawRdd.filter(x=>{
    // boolean = true
    var checkValid = true
    // 찾기: yearweek 인덱스로 주차정보만 인트타입으로 변환
    var weekValue = x.getString(yearweekNo).substring(4).toInt

    // 비교한후 주차정보가 53 이상인 경우 레코드 삭제
    if( weekValue >= 53){
      checkValid = false
    }

    checkValid
  })
  // filteredRdd.first
  // filteredRdd = (키정보, 지역정보, 상품정보, 연주차정보, 거래량 정보, 상품이름정보)

  // 처리로직 : 거래량이 MAXVALUE 이상인건은 MAXVALUE로 치환한다.


  var mapRdd = filteredRdd.map(x=>{

    // 디버깅코드: var x = mapRdd.filter(x=>{ x.getDouble(qtyNo) > 700000 }).first
    //로직구현예정

    var org_qty = x.getDouble(qtyNo)
    var new_qty = org_qty

    if(new_qty > MAXVALUE){
      new_qty = MAXVALUE
    }

    //출력 row 키정보, 연주차정보, 거래량 정보_org, 거래량 정보_new )
    Row( x.getString(keyNo),
      x.getString(yearweekNo),
      org_qty,
      new_qty
    )
  })

}
