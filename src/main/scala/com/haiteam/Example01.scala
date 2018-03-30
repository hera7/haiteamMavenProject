package com.haiteam

import org.apache.spark.sql.SparkSession

object Example01 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("jyProject").
      config("spark.master", "local").
      getOrCreate()
//
//
//    var testArray = Array(22, 33, 50, 70, 90, 100)
//
//    var answer = testArray(x=>{x%10==0})
//
//    var answer = teatArray.filter(x=>{
//      var data = x.toString
//      var dataSize = data.size
//
//      var lastChar = data.sunstring(dataSize - 1).toString
//
//      lastChar.equalsIgnoreCase("0")
//    })
//
//    var arraySize = answer.size
//    for(i<-0 until arraySize){
//      println(answer(i))
//    }

    var priceData = Array(1000,0,1200.0,1300.0,1500.0,10000.0)
    var promotionRate = 0.2
    var priceDataSize = priceData.size
    var i = 0
    while(i < priceDataSize){
      var promotionEffect = priceData(i) * promotionRate
      priceData(i) = priceData(i) - promotionEffect
      i = i+1
    }


  }

}
