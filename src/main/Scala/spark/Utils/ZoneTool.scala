package spark.Utils

import java.io.DataOutput

/**
 * @ObjectName ZoneTool
 * @Description TODO
 * @Author 马敏航
 * @Date 2021/4/13 15:23
 * @Version 1.0
 */
object ZoneTool {

  def qqsRtp(rmode: Int, pNode: Int): List[Double] = {
    if (rmode == 1 && pNode == 1) {
      List[Double](1, 0, 0)
    } else if (rmode == 1 && pNode == 2) {
      List[Double](1, 1, 0)
    } else if (rmode == 1 && pNode == 3) {
      List[Double](1, 1, 1)
    } else {
      List[Double](0, 0, 0)
    }
  }

  def jjRtp(ect: Int, bill: Int, bid: Int, iswin: Int, orderid: Int): List[Double] = {
    if (ect == 1 && bill == 1 && bid == 1 && orderid != 0) {
      List[Double](1, 0)
    } else if (ect == 1 && bill == 1 && iswin == 1) {
      List[Double](1, 1)
    } else {
      List[Double](0, 0)
    }
  }

  def ggzjRtp(rmode: Int, ect: Int): List[Double] = {
    if (rmode == 2 && ect == 1) {
      List[Double](1, 0)
    } else if (rmode == 3 && ect == 1) {
      List[Double](1, 1)
    } else {
      List[Double](0, 0)
    }
  }

  def mjjRtp(rmode: Int, ect: Int, bill: Int): List[Double] = {
    if (rmode == 2 && ect == 1 && bill == 1) {
      List[Double](1, 0)
    } else if (rmode == 3 && ect == 1 && bill == 1) {
      List[Double](1, 1)
    } else {
      List[Double](0, 0)
    }
  }

  def ggcbRtp(ect: Int, bill: Int, iswin: Int, winprice: Double, adpayment: Double): List[Double] = {
    if (ect == 1 && bill == 1 && iswin == 1) {
      List[Double](winprice * 1.0 / 1000, adpayment * 1.0 / 1000)
    } else {
      List[Double](0, 0)
    }
  }

}
