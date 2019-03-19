package cn.dmp.pojo

class Log(
           val sessionid: String,
           val advertisersid: Int,
           val adorderid: Int,
           val adcreativeid: Int,
           val adplatformproviderid: Int,
           val sdkversion: String,
           val  adplatformkey: String,
           val  putinmodeltype: Int,
           val  requestmode: Int,
           val  adprice: Double,
           val   adppprice: Double,
           val  requestdate: String,
           val  ip: String,
           val  appid: String,
           val  appname: String,
           val  uuid: String,
           val  device: String,
           val  client: Int,
           val  osversion: String,
           val  density: String,
           val pw: Int,
           val  ph: Int,
           val leng: String,
           val lat: String,
           val provincename: String,
           val cityname: String,
           val ispid: Int,
           val  ispname: String,
           val  networkmannerid: Int,
           val  networkmannername: String,
           val iseffective: Int,
           val  isbilling: Int,
           val  adspacetype: Int,
           val  adspacetypename: String,
           val  devicetype: Int,
           val  processnode: Int,
           val  apptype: Int,
           val district: String,
           val paymode: Int,
           val  isbid: Int,
           val bidprice: Double,
           val winprice: Double,
           val  iswin: Int,
           val  cur: String,
           val  rate: Double,
           val  cnywinprice: Double,
           val imei: String,
           val  mac: String,
           val idfa: String,
           val openudid: String,
           val androidid: String,
           val rtbprovince: String,
           val  rtbcity: String,
           val rtbdistrict: String,
           val rtbstreet: String,
           val storeurl: String,
           val  realip: String,
           val  isqualityapp: Int,
           val  bidfloor: Double,
           val  aw: Int,
           val ah: Int,
           val imeimd5: String,
           val macmd5: String,
           val idfamd5: String,
           val  openudidmd5: String,
           val androididmd5: String,
           val imeisha1: String,
           val macsha1: String,
           val  idfasha1: String,
           val openudidsha1: String,
           val  androididsha1: String,
           val   uuidunknow: String,
           val  userid: String,
           val  iptype: Int,
           val  initbidprice: Double,
           val  adpayment: Double,
           val agentrate: Double,
           val  lomarkrate: Double,
           val adxrate: Double,
           val title: String,
           val keywords: String,
           val  tagid: String,
           val  callbackdate: String,
           val  channelid: String,
           val  mediatype: Int
         ) extends Product with Serializable {
  override def productElement(n: Int): Any =  n match {
    case 0 => sessionid
    case 1 => advertisersid
    case 2 => adorderid
    case 3 => adcreativeid
    case 4 => adplatformproviderid
    case 5 => sdkversion
    case 6 => adplatformkey
    case 7 => putinmodeltype
    case 8 => requestmode
    case 9 => adprice
    case 10 => adppprice
    case 11 => requestdate
    case 12 => ip
    case 13 => appid
    case 14 => appname
    case 15 => uuid
    case 16 => device
    case 17 => client
    case 18 => osversion
    case 19 => density
    case 20 => pw
    case 21 => ph
    case 22 => leng
    case 23 => lat
    case 24 => provincename
    case 25 => cityname
    case 26 => ispid
    case 27 => ispname
    case 28 => networkmannerid
    case 29 => networkmannername
    case 30 => iseffective
    case 31 => isbilling
    case 32 => adspacetype
    case 33 => adspacetypename
    case 34 => devicetype
    case 35 => processnode
    case 36 => apptype
    case 37 => district
    case 38 => paymode
    case 39 => isbid
    case 40 => bidprice
    case 41 => winprice
    case 42 => iswin
    case 43 => cur
    case 44 => rate
    case 45 => cnywinprice
    case 46 => imei
    case 47 => mac
    case 48 => idfa
    case 49 => openudid
    case 50 => androidid
    case 51 => rtbprovince
    case 52 => rtbcity
    case 53 => rtbdistrict
    case 54 => rtbstreet
    case 55 => storeurl
    case 56 => realip
    case 57 => isqualityapp
    case 58 => bidfloor
    case 59 => aw
    case 60 => ah
    case 61 => imeimd5
    case 62 => macmd5
    case 63 => idfamd5
    case 64 => openudidmd5
    case 65 => androididmd5
    case 66 => imeisha1
    case 67 => macsha1
    case 68 => idfasha1
    case 69 => openudidsha1
    case 70 => androididsha1
    case 71 => uuidunknow
    case 72 => userid
    case 73 => iptype
    case 74 => initbidprice
    case 75 => adpayment
    case 76 => agentrate
    case 77 => lomarkrate
    case 78 => adxrate
    case 79 => title
    case 80 => keywords
    case 81 => tagid
    case 82 => callbackdate
    case 83 => channelid
    case 84 => mediatype
  }

  override def productArity: Int = 85

  override def canEqual(that: Any): Boolean = that.isInstanceOf[Log]
}
object Log{
  def apply(
             sessionid: String,
             advertisersid: Int,
             adorderid: Int,
             adcreativeid: Int,
             adplatformproviderid: Int,
             sdkversion: String,
             adplatformkey: String,
             putinmodeltype: Int,
             requestmode: Int,
             adprice: Double,
             adppprice: Double,
             requestdate: String,
             ip: String,
             appid: String,
             appname: String,
             uuid: String,
             device: String,
             client: Int,
             osversion: String,
             density: String,
             pw: Int,
             ph: Int,
             leng: String,
             lat: String,
             provincename: String,
             cityname: String,
             ispid: Int,
             ispname: String,
             networkmannerid: Int,
             networkmannername: String,
             iseffective: Int,
             isbilling: Int,
             adspacetype: Int,
             adspacetypename: String,
             devicetype: Int,
             processnode: Int,
             apptype: Int,
             district: String,
             paymode: Int,
             isbid: Int,
             bidprice: Double,
             winprice: Double,
             iswin: Int,
             cur: String,
             rate: Double,
             cnywinprice: Double,
             imei: String,
             mac: String,
             idfa: String,
             openudid: String,
             androidid: String,
             rtbprovince: String,
             rtbcity: String,
             rtbdistrict: String,
             rtbstreet: String,
             storeurl: String,
             realip: String,
             isqualityapp: Int,
             bidfloor: Double,
             aw: Int,
             ah: Int,
             imeimd5: String,
             macmd5: String,
             idfamd5: String,
             openudidmd5: String,
             androididmd5: String,
             imeisha1: String,
             macsha1: String,
             idfasha1: String,
             openudidsha1: String,
             androididsha1: String,
             uuidunknow: String,
             userid: String,
             iptype: Int,
             initbidprice: Double,
             adpayment: Double,
             agentrate: Double,
             lomarkrate: Double,
             adxrate: Double,
             title: String,
             keywords: String,
             tagid: String,
             callbackdate: String,
             channelid: String,
             mediatype: Int
           ): Log = new Log(sessionid, advertisersid, adorderid, adcreativeid, adplatformproviderid, sdkversion, adplatformkey, putinmodeltype, requestmode, adprice, adppprice, requestdate, ip, appid, appname, uuid, device, client, osversion, density, pw, ph, leng, lat, provincename, cityname, ispid, ispname, networkmannerid, networkmannername, iseffective, isbilling, adspacetype, adspacetypename, devicetype, processnode, apptype, district, paymode, isbid, bidprice, winprice, iswin, cur, rate, cnywinprice, imei, mac, idfa, openudid, androidid, rtbprovince, rtbcity, rtbdistrict, rtbstreet, storeurl, realip, isqualityapp, bidfloor, aw, ah, imeimd5, macmd5, idfamd5, openudidmd5, androididmd5, imeisha1, macsha1, idfasha1, openudidsha1, androididsha1, uuidunknow, userid, iptype, initbidprice, adpayment, agentrate, lomarkrate, adxrate, title, keywords, tagid, callbackdate, channelid, mediatype)

}