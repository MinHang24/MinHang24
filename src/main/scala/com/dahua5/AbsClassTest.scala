package com.dahua5

object AbsClassTest {

  def main(args: Array[String]): Unit = {
    val impl = new AbsClassImpl()
    impl.eat("菠萝")
    // 由子类进行重写的方法。
    impl.play("红鞭")

    val tr = new TraitOneImpl
    tr.hello()

  }

}
