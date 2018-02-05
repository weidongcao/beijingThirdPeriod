package com.codewars.java

/**
  * Created by CaoWeiDong on 2018-01-30.
  */
object ImplicitClassDemo {
    implicit class MyImplicitTypeConversion(val str: String){
        def strToInt = str.toInt
    }

    def main(args: Array[String]): Unit = {

    }

}
