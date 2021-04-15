package com.dahua3

import scala.collection.mutable


/**
  * Set集合特征：无序的，不可重复的。不可变集合。
  *
  * import scala.collection.mutable._
  */
object SetDemo extends App {

  var set:Set[Int] = Set(4, 7, 9, 1, 12, 12, 56, 34, 78)
  for (ele <- set) {
    print(ele + " ")
  }

  println("============================")

  // 可以用可变集合。
  val hashSet: mutable.HashSet[Int] = mutable.HashSet[Int]()
  hashSet += (12)
  hashSet += (3)
  hashSet += (5)
  println(hashSet)

  println("==========================")
  // 所有的集合都可相互转换。
  private val list: List[Int] = set.toList

     Set("a","b","c")
    .zipWithIndex    //(a,0), (b,1), (c,2)
    .map{case(v,i) => (v, i+1)}  //increment each of those indexes
    .toMap
     private val set1 = Set("a","b","c")
     private val set2 = Set(1,2,3)
     private val map: Map[String, Int] = set1.zip(set2).toMap
     println(map)






}
