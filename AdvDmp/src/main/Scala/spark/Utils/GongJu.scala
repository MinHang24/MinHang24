package spark.Utils

object GongJu {

  //获取唯一的id值
  def getKey(imei: String, mac: String, idfa: String, openudid: String, androidid: String): String = {
    if (!imei.isEmpty) {
      imei
    } else if (!mac.isEmpty) {
      mac
    } else if (!idfa.isEmpty) {
      idfa
    } else if (!openudid.isEmpty) {
      openudid
    } else {
      androidid
    }
  }


  //广告位的类型标签
  def adspace(adspacetype:Int):String ={
    if (adspacetype < 10){
      "LC0"+adspacetype+"->"
    }else {
      "LC0"+adspacetype+"->"
    }
  }

  //广告位的类型标签名称
  def adspacename(adspacetypename:String):String ={
    if (adspacetypename == "banner"){
     "LN"+adspacetypename+"->"
    }else if (adspacetypename == "插屏"){
      "LN"+adspacetypename+"->"
    }else if(adspacetypename == "全屏"){
      "LN"+adspacetypename+"->"
    }else{
      "LN"+adspacetypename+"->"
    }
  }

}

