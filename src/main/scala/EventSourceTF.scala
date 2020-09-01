package ai.jubi


import java.util.concurrent.CompletableFuture

import org.apache.spark.streaming.dstream.DStream


object EventSourceTF {





  def run(rawStream:DStream[String],summaryStream: DStream[(String, String, String, String, String, String,String,String,String,String)]): Unit = {
      saveRaw(rawStream)
      saveSummary(summaryStream)
//    rawStream.foreachRDD(_.collect.foreach(println))
  }

  private def saveRaw(stream: DStream[String]):Unit = {
    stream
      .foreachRDD((rdd,sparkTime)=>{
        Helper.printTaskDelay(s"eventSource - ${rdd.getNumPartitions} partitions",sparkTime,Configurations.sessionBatchIntervalInSeconds)
        Sink.runWithoutKafka(rdd,Values.TAG_EVENT_SOURCE,Helper.fetchTimestampFromSparkTime(sparkTime))
      })
  }

  private def saveSummary(stream: DStream[(String, String, String, String, String, String,String,String,String,String)]): Unit = {
    stream
    .foreachRDD((rdd,sparkTime) =>{
      Helper.printTaskDelay(s"allEvents - ${rdd.getNumPartitions} partitions",sparkTime,Configurations.sessionBatchIntervalInSeconds)
      Sink.runWithPartitions(rdd.map{
        case (projectId,channel,senderId,status,prevConversationType,previousStageName,intentName,intentDetected,stageName,timestamp)=>
          s"{" +
            s"${Values.PROJECT_ID}:${Helper.jsonFriendlyVar(projectId)}," +
            s"${Values.CHANNEL}:${Helper.jsonFriendlyVar(channel)},"+
            s"${Values.TIMESTAMP}:${timestamp.toLong}," +
            s"${Values.SENDER_ID}:${Helper.jsonFriendlyVar(senderId)}" +
            s"${Values.STATUS}:${Helper.jsonFriendlyVar(status)}," +
            s"${Values.PREV_CONVERSATION_TYPE}:${Helper.jsonFriendlyVar(prevConversationType)}," +
            s"${Values.PREV_STAGE}:${Helper.jsonFriendlyVar(previousStageName)}," +
            s"${Values.INTENT_NAME}:${Helper.jsonFriendlyVar(intentName)}," +
            s"${Values.INTENT_DETECTED}:${Helper.jsonFriendlyVar(intentDetected)}," +
            s"${Values.STAGE_NAME}:${Helper.jsonFriendlyVar(stageName)}," +
            s"${Values.TOPIC}:${Helper.jsonFriendlyVar(Values.TAG_ALL_EVENTS)}" +
            s"}"
      }, Values.TAG_ALL_EVENTS,Helper.fetchTimestampFromSparkTime(sparkTime))
    })
  }
}
