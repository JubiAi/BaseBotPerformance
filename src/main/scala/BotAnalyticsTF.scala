package ai.jubi


import java.util.concurrent.CompletableFuture

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, Time}
import org.apache.spark.streaming.dstream.DStream

object BotAnalyticsTF{


  def run(stream: DStream[(String, String, String, String, String, String,String,String,String,String)]): Unit = {
//    runEngagement(stream.map { case (projectId, channel, senderId, _, _, _, _, _, _, timestamp) => (projectId, channel, senderId, timestamp) })
    runPerformance(stream.map { case (projectId, channel, senderId, status, _, _, intentName, intentDetected, stageName, timestamp) => (projectId, channel, senderId, status, intentName, intentDetected, stageName, timestamp) })
//    runDetailed(stream)
  }

  def runEngagement(dataStream: DStream[(String,String, String, String)]): Unit = {
    dataStream
      .foreachRDD((rdd,sparkTime)=>{
        Helper.printTaskDelay(s"engagement - ${rdd.getNumPartitions} partitions",sparkTime,Configurations.sessionBatchIntervalInSeconds)
        countMessages(rdd,sparkTime)
        countUsers(rdd,sparkTime)
        calcTotalTimeSpent(rdd,sparkTime)
        countSessions(rdd,sparkTime)
      })
  }

  def runPerformance(dataStream: DStream[(String,String, String, String, String, String, String,String)]): Unit = {
    dataStream
      .foreachRDD((rdd,sparkTime) => {
        Helper.printTaskDelay(s"performance - ${rdd.getNumPartitions} partitions",sparkTime,Configurations.sessionBatchIntervalInSeconds)
        countResponseType(rdd.map{case (projectId,channel,_,status,_,_,_,timestamp) =>(projectId,channel,status,timestamp)},sparkTime)
        val dropRdd=rdd
          .map{case (projectId,channel,senderId,_,intentName,_,stageName,timestamp) =>(projectId,channel,senderId,intentName,stageName,timestamp)}
          .filter{case (_,_,_,intentName,_,_) => Helper.checkIfPresent(intentName)}
          .map{case (projectId,channel,senderId,intentName,stageName,timestamp) =>((projectId,channel,Helper.formatTime(timestamp,Configurations.sessionBatchIntervalInSeconds),senderId),(intentName,stageName,timestamp.toLong))}
          .mapPartitions(_.toList.sortBy(_._2._3.toLong).toIterator)
        //          .cache()
        countDropIntents(dropRdd,sparkTime)
        countDropStages(dropRdd,sparkTime)
      })
  }

  def runDetailed(dataStream: DStream[(String,String, String, String, String, String, String,String,String,String)]): Unit = {
    dataStream
      .foreachRDD((rdd,sparkTime)=>{
        Helper.printTaskDelay(s"detailed - ${rdd.getNumPartitions} partitions",sparkTime,Configurations.sessionBatchIntervalInSeconds)
        countJourneyNodes(rdd,sparkTime)
        countIntentTriggers(rdd.map{case (projectId,channel,_,status,_,_,intentName,_,intentDetected,timestamp) =>(projectId,channel,status,intentName,intentDetected,timestamp)},sparkTime)
        saveOpenSessions(rdd,sparkTime)
      })
  }

  private def countJourneyNodes(rdd: RDD[(String,String, String,String, String, String, String,String,String,String)],sparkTime:Time): Unit = {
    val journeyNodesRdd=rdd
      .filter{case (_,_,_,status,prevConversationType,previousStageName,intentName,_,stageName,_) => Helper.checkIfPresent(intentName)&&Helper.checkIfPresent(stageName)&&Helper.checkIfPresent(previousStageName)&&Helper.checkIfPresent(prevConversationType)&&(status.equals("inFlowNextValidated")||status.equals("inFlowNextInvalidated"))&&prevConversationType=="flow"}
      .map{case (projectId,channel,senderId,status,_,previousStageName,intentName,_,stageName,timestamp)=>((projectId,channel,Helper.formatTime(timestamp,Configurations.sessionBatchIntervalInSeconds),senderId),(status,previousStageName,intentName,stageName,timestamp))}
      .map{case (id,(status,previousStageName,intentName,stageName,_))=>
        if(status.equals("inFlowNextInvalidated")){
          (id,"invalidNode",intentName,previousStageName,stageName)
        }
        else{
          (id,"validNode",intentName,previousStageName,stageName)
        }
      }
      .map{case((projectId,channel,timeMark,_),state,intentName,previousStageName,stageName)=>((projectId,channel,timeMark,state,intentName,previousStageName,stageName),1)}
      .reduceByKey(_+_)


    Sink.run(journeyNodesRdd
      .map{case ((projectId,channel,timeMark,state,intentName,previousStageName,stageName),count:Int) =>
        s"{" +
          s"${Values.PROJECT_ID}:${Helper.jsonFriendlyVar(projectId)}," +
          s"${Values.TIME_MARK}:${timeMark}," +
          s"${Values.COUNT}:$count," +
          s"${Values.CHANNEL}:${Helper.jsonFriendlyVar(channel)},"+
          s"${Values.STAGE_NAME}:${Helper.jsonFriendlyVar(stageName)},"+
          s"${Values.PREV_STAGE}:${Helper.jsonFriendlyVar(previousStageName)},"+
          s"${Values.STATE}:${Helper.jsonFriendlyVar(state)},"+
          s"${Values.INTENT_NAME}:${Helper.jsonFriendlyVar(intentName)}," +
          s"${Values.WINDOW}:${Configurations.sessionBatchIntervalInSeconds*1000}," +
          s"${Values.TOPIC}:${Helper.jsonFriendlyVar(Values.TAG_JOURNEY_NODES)}" +
          s"}"
      },Values.TAG_JOURNEY_NODES+"_"+Configurations.sessionBatchIntervalInSeconds.toString,Helper.fetchTimestampFromSparkTime(sparkTime))
  }

  private def countDropIntents(rdd: RDD[((String,String,Long,String),(String,String,Long))],sparkTime:Time):Unit={
    val dropOffRdd = rdd
      .mapValues{case (intentName,_,timestamp)=>(intentName,timestamp)}
      .groupByKey
      .filter{case (_,listOfIntentTimestamps) =>listOfIntentTimestamps.size!=0}
      .mapValues{
        case listInput=>
          val list=listInput.filter(_!=null).filter(_._1!=null)
          (if (list.size>=2) list.maxBy{case (_,timestamp)=>timestamp}._1 else list.toList(0)._1)
      }
      .map{case ((projectId,channel,timeMark,_),intentName)=>((projectId,channel,timeMark,intentName),1)}
      .reduceByKey(_+_)

    Sink.run(dropOffRdd
      .map{case ((projectId,channel,timeMark,intentName),count) =>
        s"{" +
          s"${Values.PROJECT_ID}:${Helper.jsonFriendlyVar(projectId)}," +
          s"${Values.CHANNEL}:${Helper.jsonFriendlyVar(channel)},"+
          s"${Values.TIME_MARK}:${timeMark}," +
          s"${Values.COUNT}:$count," +
          s"${Values.INTENT_NAME}:${Helper.jsonFriendlyVar(intentName)}," +
          s"${Values.WINDOW}:${Configurations.sessionBatchIntervalInSeconds*1000}," +
          s"${Values.TOPIC}:${Helper.jsonFriendlyVar(Values.TAG_DO_INTENT_COUNT)}" +
          s"}"
      },Values.TAG_DO_INTENT_COUNT+"_"+Configurations.sessionBatchIntervalInSeconds.toString,Helper.fetchTimestampFromSparkTime(sparkTime))
  }

  private def countDropStages(rdd: RDD[((String,String,Long,String),(String,String,Long))],sparkTime:Time):Unit={
    val dropOffRdd = rdd
      .groupByKey
      .filter{case (_,listOfStageTimestamps) =>listOfStageTimestamps.size!=0}
      .mapValues{
        case listInput=>
          val list=listInput.filter(_!=null).filter(_._1!=null).filter(_._2!=null)
          (

            if (list.size>=2) {
              val maxStageTimestamp = list.maxBy{case (_,_,timestamp)=>timestamp}
              (maxStageTimestamp._1,maxStageTimestamp._2)
            }else{
              (list.toList(0)._1,list.toList(0)._2)
            }
            )
      }
      .map{case ((projectId,channel,timeMark,_),(intentName,stageName))=>((projectId,channel,timeMark,intentName,stageName),1)}
      .reduceByKey(_+_)

    Sink.run(dropOffRdd
      .map{case ((projectId,channel,timeMark,intentName,stageName),count) =>
        s"{" +
          s"${Values.PROJECT_ID}:${Helper.jsonFriendlyVar(projectId)}," +
          s"${Values.CHANNEL}:${Helper.jsonFriendlyVar(channel)},"+
          s"${Values.TIME_MARK}:${timeMark}," +
          s"${Values.COUNT}:$count," +
          s"${Values.INTENT_NAME}:${Helper.jsonFriendlyVar(intentName)}," +
          s"${Values.STAGE_NAME}:${Helper.jsonFriendlyVar(stageName)}," +
          s"${Values.WINDOW}:${Configurations.sessionBatchIntervalInSeconds*1000}," +
          s"${Values.TOPIC}:${Helper.jsonFriendlyVar(Values.TAG_DO_STAGE_COUNT)}" +
          s"}"
      },Values.TAG_DO_STAGE_COUNT+"_"+Configurations.sessionBatchIntervalInSeconds.toString,Helper.fetchTimestampFromSparkTime(sparkTime))
  }

  private def countIntentTriggers(rdd: RDD[(String,String, String, String,String,String)],sparkTime:Time): Unit = {
    val intentRdd=rdd
      .map{case (projectId,channel,status,intentName,intentDetected,timestamp) =>((projectId,channel,Helper.formatTime(timestamp,Configurations.sessionBatchIntervalInSeconds)),(
        if(Helper.checkIfPresent(intentDetected)&&Helper.checkIfPresent(status)&&status.equals("inFlowNextGhost")){
          intentDetected
        }else if(Helper.checkIfPresent(status)&&Helper.checkIfPresent(intentName)&&status.equals("nextStart")){
          intentName
        }else{
          ""
        }))}
      .filter{case (_,intentName) => Helper.checkIfPresent(intentName)}
      .map{case ((projectId,channel,timeMark),intentName) =>((projectId,channel,timeMark,intentName),1)}
      .reduceByKey(_+_)

    Sink.run(intentRdd
      .map{case ((projectId:String,channel,timeMark:Long,intentName:String),count:Int) =>
        s"{" +
          s"${Values.PROJECT_ID}:${Helper.jsonFriendlyVar(projectId)}," +
          s"${Values.TIME_MARK}:${timeMark}," +
          s"${Values.CHANNEL}:${Helper.jsonFriendlyVar(channel)},"+
          s"${Values.COUNT}:$count," +
          s"${Values.INTENT_NAME}:${Helper.jsonFriendlyVar(intentName)}," +
          s"${Values.WINDOW}:${Configurations.sessionBatchIntervalInSeconds*1000}," +
          s"${Values.TOPIC}:${Helper.jsonFriendlyVar(Values.TAG_INTENT_TRIGGER_COUNT)}" +
          s"}"
      },Values.TAG_INTENT_TRIGGER_COUNT+"_"+Configurations.sessionBatchIntervalInSeconds.toString,Helper.fetchTimestampFromSparkTime(sparkTime))
  }

  private def countResponseType(rdd: RDD[(String,String, String, String)],sparkTime:Time):Unit={
    val responseTypeRdd=rdd.map{case (projectId,channel,status,timestamp) =>
      ((projectId,
        channel,
        Helper.formatTime(timestamp,Configurations.sessionBatchIntervalInSeconds),
        if(List("cancel" ).contains(status)){
          Values.CANCELLED_TYPE
        }
        else if(List("nextStart").contains(status)){
          Values.INTENT_TRIGGERED_TYPE
        }
        else if(List("inFlowNextValidated", "inFlowPrevious").contains(status)){
          Values.JOURNEY_VALID_TYPE
        }
        else if(List("inFlowNextInvalidated").contains(status)){
          Values.JOURNEY_INVALID_TYPE
        }
        else if(List("inFlowNextGhost").contains(status)){
          Values.JOURNEY_GHOST_TYPE
        }
        else if(List("cancelStuck").contains(status)){
          Values.JOURNEY_STUCK_TYPE
        }
        else if(List("nextOptions").contains(status)){
          Values.OPTIONS_TYPE
        }
        else{
          Values.FALLBACK_TYPE
        }
      ),1)
    }
      .reduceByKey(_+_)


    Sink.run(responseTypeRdd
      .map{case ((projectId,channel,timeMark,respType),count)=>
        s"{" +
          s"${Values.PROJECT_ID}:${Helper.jsonFriendlyVar(projectId)}," +
          s"${Values.TIME_MARK}:${timeMark}," +
          s"${Values.CHANNEL}:${Helper.jsonFriendlyVar(channel)},"+
          s"${Values.COUNT}:$count," +
          s"${Values.RESPONSE_TYPE}:${Helper.jsonFriendlyVar(respType)}," +
          s"${Values.WINDOW}:${Configurations.sessionBatchIntervalInSeconds*1000}," +
          s"${Values.TOPIC}:${Helper.jsonFriendlyVar(Values.TAG_RESP_TYPE)}" +
          s"}"
      },Values.TAG_RESP_TYPE+"_"+Configurations.sessionBatchIntervalInSeconds.toString,Helper.fetchTimestampFromSparkTime(sparkTime))

  }

  private def countUsers(rdd: RDD[(String, String,String, String)],sparkTime:Time): Unit ={

    val userRdd = rdd
      .map{case (projectId,channel,senderId,timestamp) => (projectId ,channel, Helper.formatTime(timestamp,Configurations.sessionBatchIntervalInSeconds),senderId)}
      .distinct

    Sink.run(userRdd.map{
      case ((projectId,channel,timeMark,senderId))=>
        s"{" +
          s"${Values.PROJECT_ID}:${Helper.jsonFriendlyVar(projectId)}," +
          s"${Values.CHANNEL}:${Helper.jsonFriendlyVar(channel)},"+
          s"${Values.TIME_MARK}:${timeMark}," +
          s"${Values.SENDER_ID}:${Helper.jsonFriendlyVar(senderId)}," +
          s"${Values.WINDOW}:${Configurations.sessionBatchIntervalInSeconds*1000}," +
          s"${Values.TOPIC}:${Helper.jsonFriendlyVar(Values.TAG_USER)}" +
          s"}"
    },Values.TAG_USER+"_"+Configurations.sessionBatchIntervalInSeconds.toString,Helper.fetchTimestampFromSparkTime(sparkTime))



  }

  private def countMessages(rdd: RDD[(String,String, String, String)],sparkTime:Time): Unit ={
    val messageRdd = rdd .map {case (projectId,channel,_,timestamp) =>((projectId,channel,Helper.formatTime(timestamp,Configurations.sessionBatchIntervalInSeconds)), 1L)}.reduceByKey(_ + _)
    Sink.run(messageRdd.map(data=>{
      sinkCount(data,Values.COUNT,Values.TAG_MSG)
    }), Values.TAG_MSG+"_"+Configurations.sessionBatchIntervalInSeconds.toString,Helper.fetchTimestampFromSparkTime(sparkTime))
  }

  private def calcTotalTimeSpent(rdd: RDD[(String,String, String, String)],sparkTime:Time): Unit ={
    val totalTimeRdd = rdd
      .map{case (projectId,channel,senderId,timestamp) => ((projectId,channel,Helper.formatTime(timestamp,Configurations.sessionBatchIntervalInSeconds),  senderId),timestamp.toLong)}
      .mapPartitions(_.toList.sortBy(_._2.toLong).toIterator)
      .groupByKey
      .map{case ((projectId,channel,timeMark,_),list)=>((projectId,channel,timeMark),(list.max-list.min))}
      .filter{case (_,time)=>time<=Configurations.sessionBatchIntervalInSeconds*1000}
      .reduceByKey{case (timeDiff1,timeDiff2)=>timeDiff1+timeDiff2}

    Sink.run(totalTimeRdd.map(data=>{
      sinkCount(data,Values.TOTAL_TIME,Values.TAG_TIME_SPENT)
    }),  Values.TAG_TIME_SPENT+"_"+Configurations.sessionBatchIntervalInSeconds.toString,Helper.fetchTimestampFromSparkTime(sparkTime))

  }

  private def saveOpenSessions(rdd: RDD[(String, String, String,String, String, String,String, String, String,String)],sparkTime:Time): Unit ={

    val sessionBaseRdd = rdd.map{case (projectId,channel,senderId,status,prevConversationType,previousStageName,intentName,intentDetected,stageName,timestamp) => ((projectId, channel,Helper.formatTime(timestamp,Configurations.sessionBatchIntervalInSeconds),senderId),(status,prevConversationType,previousStageName,intentName,intentDetected,stageName,timestamp.toLong))}

    val sessionMaxTimestampRdd=sessionBaseRdd
      .reduceByKey((a,b)=>{
        val maxVal=math.max(a._7, b._7)
        if(a._7==maxVal){
          a
        }
        else{
          b
        }
      })
      .filter{case ((_,_,timeMark,_),(_,_,_,_,_,_,timestamp))=>(timeMark-timestamp)<=Configurations.sessionBatchIntervalInSeconds*1000}
    Sink.run(sessionMaxTimestampRdd.map{
      case ((projectId,channel,timeMark,senderId),(status,prevConversationType,previousStageName,intentName,intentDetected,stageName,maxTimestamp))=>
        s"{" +
          s"${Values.PROJECT_ID}:${Helper.jsonFriendlyVar(projectId)}," +
          s"${Values.CHANNEL}:${Helper.jsonFriendlyVar(channel)},"+
          s"${Values.TIME_MARK}:${timeMark}," +
          s"${Values.SENDER_ID}:${Helper.jsonFriendlyVar(senderId)}," +
          s"${Values.STATUS}:${Helper.jsonFriendlyVar(status)}," +
          s"${Values.PREV_CONVERSATION_TYPE}:${Helper.jsonFriendlyVar(prevConversationType)}," +
          s"${Values.PREV_STAGE}:${Helper.jsonFriendlyVar(previousStageName)}," +
          s"${Values.INTENT_NAME}:${Helper.jsonFriendlyVar(intentName)}," +
          s"${Values.INTENT_DETECTED}:${Helper.jsonFriendlyVar(intentDetected)}," +
          s"${Values.STAGE_NAME}:${Helper.jsonFriendlyVar(stageName)}," +
          s"${Values.MAX_TS}:${maxTimestamp}," +
          s"${Values.WINDOW}:${Configurations.sessionBatchIntervalInSeconds*1000}," +
          s"${Values.TOPIC}:${Helper.jsonFriendlyVar(Values.TAG_OPEN_SESSIONS)}" +
          s"}"
    },  Values.TAG_OPEN_SESSIONS+"_"+Configurations.sessionBatchIntervalInSeconds.toString,Helper.fetchTimestampFromSparkTime(sparkTime))


    val sessionMinTimestampRdd=sessionBaseRdd
      .reduceByKey((data1,data2)=>{
        val minVal=math.min(data1._7, data2._7)
        if(data1._7==minVal){
          data1
        }
        else{
          data2
        }
      })
      .filter{case ((_,_,timeMark,_),(_,_,_,_,_,_,timestamp))=>(timeMark-timestamp)<=Configurations.sessionBatchIntervalInSeconds*1000}
    Sink.run(sessionMinTimestampRdd.map{
      case ((projectId,channel,timeMark,senderId),(status,prevConversationType,previousStageName,intentName,intentDetected,stageName,minTimestamp))=>
        s"{" +
          s"${Values.PROJECT_ID}:${Helper.jsonFriendlyVar(projectId)}," +
          s"${Values.CHANNEL}:${Helper.jsonFriendlyVar(channel)},"+
          s"${Values.TIME_MARK}:${timeMark}," +
          s"${Values.SENDER_ID}:${Helper.jsonFriendlyVar(senderId)}," +
          s"${Values.STATUS}:${Helper.jsonFriendlyVar(status)}," +
          s"${Values.PREV_CONVERSATION_TYPE}:${Helper.jsonFriendlyVar(prevConversationType)}," +
          s"${Values.PREV_STAGE}:${Helper.jsonFriendlyVar(previousStageName)}," +
          s"${Values.INTENT_NAME}:${Helper.jsonFriendlyVar(intentName)}," +
          s"${Values.INTENT_DETECTED}:${Helper.jsonFriendlyVar(intentDetected)}," +
          s"${Values.STAGE_NAME}:${Helper.jsonFriendlyVar(stageName)}," +
          s"${Values.MIN_TS}:${minTimestamp}," +
          s"${Values.WINDOW}:${Configurations.sessionBatchIntervalInSeconds*1000}," +
          s"${Values.TOPIC}:${Helper.jsonFriendlyVar(Values.TAG_OPEN_SESSIONS)}" +
          s"}"
    },  Values.TAG_OPEN_SESSIONS+"_"+Configurations.sessionBatchIntervalInSeconds.toString,Helper.fetchTimestampFromSparkTime(sparkTime))

  }

  private def countSessions(rdd: RDD[(String,String, String, String)],sparkTime:Time): Unit ={
    val sessionCountRdd = rdd
      .map{case (projectId,channel,senderId,timestamp) => ((projectId,channel, Helper.formatTime(timestamp,Configurations.sessionBatchIntervalInSeconds),senderId),timestamp.toLong)}
      .mapPartitions(_.toList.sortBy(_._2.toLong).toIterator)
      .groupByKey
      .map{case ((projectId,channel,timeMark,_),list) => ((projectId,channel,timeMark),
        if (list.size>=2) {
          list
            .sliding(2, 1)
            .map(_.toList)
            .map(data => if ((data(1) - data(0)) > Configurations.sessionBatchIntervalInSeconds * 1000) 1L else 0L)
            .reduceOption(_ + _).getOrElse(0L) + 1L
        }
        else{1L}
      )}
      .reduceByKey(_+_)
    Sink.run(sessionCountRdd.map(data=>{
      sinkCount(data,Values.COUNT,Values.TAG_SESSION)
    }),  Values.TAG_SESSION+"_"+Configurations.sessionBatchIntervalInSeconds.toString,Helper.fetchTimestampFromSparkTime(sparkTime))
  }

  private def sinkCount(data: ((String,String,Long), Long),typeVal:String,topic:String): String =  s"{${Values.PROJECT_ID}:${Helper.jsonFriendlyVar(data._1._1)},${Values.CHANNEL}:${Helper.jsonFriendlyVar(data._1._2)},${Values.TIME_MARK}:${data._1._3},$typeVal:${data._2},${Values.TOPIC}:${Helper.jsonFriendlyVar(topic)},${Values.WINDOW}:${Configurations.sessionBatchIntervalInSeconds*1000}}"

}