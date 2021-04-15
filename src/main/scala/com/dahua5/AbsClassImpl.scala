package com.dahua5

class AbsClassImpl extends AbstractClass {
  override
  def eat(str: String): Unit = {
    println("喜欢吃的水果："+str)
  }

  override def play(str: String): Unit = {
    println("喜欢看的电影："+str);
  }
}
