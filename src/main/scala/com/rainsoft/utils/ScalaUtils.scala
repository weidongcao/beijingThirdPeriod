package com.rainsoft.utils

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by CaoWeiDong on 2017-12-06.
  */
object ScalaUtils {
    def convertJavaListToScala[Object](list: java.util.List[Object]): Seq[Object] = {
        return list.asScala.toSeq
    }

    def main(args: Array[String]): Unit = {
        val list = new util.ArrayList[String]()
        list.add("aaa")
        list.add("sss")
        list.add("kkk")

        list.asScala
        print(list)
    }
}
