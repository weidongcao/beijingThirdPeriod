package com.codewars.java

/**
  * Created by CaoWeiDong on 2018-01-30.
  */
object ImplicitFunDemo {
    object MyImplicit{
        implicit def strToInt(str: String) = str.toInt
    }

    def main(args: Array[String]): Unit = {
        import MyImplicit.strToInt

        val max = math.max("3", 1)
    }

}
