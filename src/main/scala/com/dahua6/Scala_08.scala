package com.dahua6

/**
  * Scala的模式匹配：
  * 就是像java中Switch case
  */
object Scala_08 {

  def main(args: Array[String]): Unit = {
    // 匹配字符串内容
    def stringMatch(str: String) = str match {
      case "hello" => println("hello scala")
      case "dog" => println("hello dog")
      case "1" => println("hello 2")
      case _ => println("匹配无效")
    }

    stringMatch("hello")
    stringMatch("dog")
    stringMatch("2")

    println("===============================")

    // 匹配：类型
    def typeMatch(ty: Any) = ty match {
      case x: Int => println(s"Int${x}")
      case y: Double => println(s"double${y}")
      case _ => println("无效匹配")
    }

    typeMatch(123)
    typeMatch(123.15)

    // 匹配数组
    println("----------匹配数组---------")

    def arrayMatch(arr: Any) = arr match {
      case Array(0) => println("空数组")
      case Array(0, _) => println("以0开头,拥有两个元素")
      case Array(0, _, 3) => println("以0开头,以3结尾,中间有无限其他元素")
      case Array(0, _*) => println("以0开头,最后有无限其他元素")
    }

    arrayMatch(Array(0))
    arrayMatch(Array(0, 2))
    arrayMatch(Array(0, 9, 3))
    arrayMatch(Array(0, 9, 4, 5, 6, 7, 89, 100))

    println("----------匹配List集合---------")

    def listMatch(list: Any) = list match {
      case 0 :: Nil => println("只有一个0的集合")
      case 0 :: 7 :: Nil => println("只有0和7的集合")
      case x :: y :: z :: Nil => println("任意三个元素的集合")
      case m :: n if n.length > 0 => println("---------")
      case _ => println("任意集合")
    }

    listMatch(List(0))
    listMatch(List(0, 7))
    listMatch(List(0, "true", true))
    listMatch(List("hello", "true"))
    listMatch(new Stu)

    println("-------------匹配元组-----------")

    def tupleMatch(tuple: Any) = tuple match {
      case (0, _) => println("以0开头的一个元组,第二个值任意")
      case (x, y, z) => println("拥有任意3个元素的元组")
      case (_, "tom") => println("第二元素必须是tom")
      case _ => println("任意值")
    }

    tupleMatch((0, "jack"))
    tupleMatch((1, "tom"))
    tupleMatch((1, "lucy",true))

    println("------------任意匹配------------")

    def nothingMatch(nothing:Any)=nothing match{
      case "hello" => println("你是字符串")
      case x:Int => println("你是数字:"+x)
      case Array(_*) =>
        val ints: Array[Int] = nothing.asInstanceOf[Array[Int]]
        ints.foreach(println)

    }

    nothingMatch("hello")
    nothingMatch(Array(0,1,2,32))

    println("--------------匹配对象-------------")
    def objMatch(obj:Any)=obj match{
      case Stu()=> println("学生类型")
      case Teacher() => println("老师类型")
      case Major() => println("专业类型")
    }

    objMatch(new Stu())
    objMatch(new Teacher())
    objMatch(new Major())

  }

}

case class Stu() {}
case class Teacher(){}
case class Major(){}
