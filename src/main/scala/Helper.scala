package ai.jubi

import java.time.{Instant, ZoneId, ZonedDateTime}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.streaming.{StreamingContext, Time}



object Helper extends Serializable {

  @transient lazy val log = Logger.getLogger(getClass.getName)

  def formatTime(timeMark:String,timeInterval:Long):Long = ((timeMark.toLong / (timeInterval*1000))+1)*timeInterval*1000

  def jsonFriendlyVar(data:String):String='"'+data+'"'

  def fetchTimestampFromSparkTime(time:Time):String=time.toString.split("\\s")(0)

  def getTimeZone(timestamp:Long):String=ZonedDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("Asia/Calcutta")).toString

  def checkIfPresent(value:String):Boolean=value!=null&&value!="EMPTY"&&(!value.equals(""))

  def assignValueIfNotPresent(value:String):String=if(value!=null&&(!value.equals(""))){value}else{"EMPTY"}

  def printTaskDelay(name:String,sparkBatchTime:Time,taskBatchInterval:Long)={
    printAndLog(s"\n_______________________${name}________________________")
    val fromTime =this.fetchTimestampFromSparkTime(sparkBatchTime).toLong-taskBatchInterval*1000
    val toTime =this.fetchTimestampFromSparkTime(sparkBatchTime).toLong
    printAndLog(s"Executing for time period ${this.getTimeZone(fromTime)} - ${this.getTimeZone(toTime)}")
    printAndLog(s"Delay of ${(System.currentTimeMillis()-toTime)/1000} secs")
    printAndLog(s"_______________________${name}________________________\n")

  }

  def setupLogging():Unit = {
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  }

  def printAndLog(text:String): Unit ={
    println(text)
    log.info(text)
  }

  def gracefulAwaitTermination(ssc: StreamingContext):Unit={
    var isStopped = false
    var stopFlag  = false
    while (! isStopped) {
      isStopped = ssc.awaitTerminationOrTimeout(10000)
      if (isStopped) {
        ssc.stop(true, true)
        Helper.printAndLog("Confirmed! The streaming context is stopped. Exiting application...")
      }
      stopFlag=checkShutdownMarker(stopFlag)
      if (!isStopped && stopFlag) {
        Helper.printAndLog("Stopping ssc right now")
        ssc.stop(true, true)
        Helper.printAndLog("----------Ssc is stopped!---------")
      }
    }
  }

  def checkShutdownMarker(stopFlag:Boolean):Boolean = {
    if (!stopFlag) {
      val fs = FileSystem.get(new Configuration())
      !fs.exists(new Path(Configurations.shutdownMarker))
    }
    else {
      stopFlag
    }
  }


}

