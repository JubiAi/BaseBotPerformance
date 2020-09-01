package ai.jubi

import org.apache.kafka.common.serialization.StringDeserializer

object Configurations {

  val topics =Array("BotMessageEventsTopic")

  val checkpointDir="hdfs://etl:9000/base-bot-stream-0/"

  val shutdownMarker = "hdfs://etl:9000/sparkShutdownMarkers-base-bot-stream-0"

  val kafkaConsumeUri = "kafka1:9092,kafka2:9092"

  val appName = "BaseBotPerformance"

  val groupId = appName

  val uiPort ="4040"

  val metricsPort = "72" // "81"

  val memory = "1g"

  val instances = "4"

  val checkpoint = false // true

  val sessionBatchIntervalInSeconds = 5*60

  val partitions = Configurations.instances.toInt

  val kafkaProduceUri="kafka1:9092,kafka2:9092"

  val dbUri=(collection:String)=>s"mongodb+srv://eventSourceWriteOnly:5duoskQ0CMTWrRh9@sharedparramato-sy1qy.mongodb.net/shared-parramato.${collection}?authSource=admin&replicaSet=sharedParramato-shard-0&w=majority&readPreference=primary&appname=MongoDB%20Compass&retryWrites=true&ssl=true"

  val storageUri=(topic:String,sparkTimestamp:String)=>s"hdfs://etl:9000/parramato/${topic}/${sparkTimestamp}/"

  val printOnly = true // false

  val schemaElements=Array( "projectId", "filterCode", "senderId", "payload.status.final", "payload.status.prevConversation", "payload.status.previousStage", "conversationId", "intentName", "stageName", "timestamp", "requestAssistance")

  val schemaInit=("", "", "", "", "", "","","", "","","")

  val sparkSettings = Map(
    "spark.streaming.receiver.writeAheadLog.enable"-> "true",
    "spark.sql.session.timeZone"-> "IST",
    "spark.ui.port"->Configurations.uiPort,
    "spark.executor.extraJavaOptions"->s"-Duser.timezone=IST -XX:+UseCompressedOops -XX:+UseG1GC -javaagent:/usr/local/spark/jars/jmx_prometheus_javaagent-0.13.0.jar=${Configurations.metricsPort}:/usr/local/spark/conf/spark.yml",
    "spark.ui.prometheus.enabled"->"true",
    "park.eventLog.logStageExecutorMetrics"->"true",
    "spark.eventLog.enabled"->  "true",
    "spark.executor.instances"-> Configurations.instances,
    "spark.executor.memory"-> Configurations.memory,
    "spark.cores.max"-> Configurations.instances,
    "spark.memory.fraction"-> "0.3",
    "spark.cleaner.referenceTracking.blocking"-> "false",
    "spark.cleaner.periodicGC.interval"-> "5min",
    "spark.debug.maxToStringFields"->"200",
    "spark.network.timeout"->"240s",
    "spark.executor.heartbeatInterval"-> "30s",
    "spark.cleaner.referenceTracking.cleanCheckpoints"->"true",
    "spark.task.maxFailures" -> "6"

//    "spark.streaming.concurrentJobs"-> "4",
//    "spark.scheduler.mode" -> "FAIR",
//    "spark.streaming.receiver.maxRate" -> "3000",
//    "spark.streaming.backpressure.enabled" -> "true",

  )
  val kafkaParams =  Map[String, Object](
    "bootstrap.servers" -> Configurations.kafkaConsumeUri,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> Configurations.groupId,
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )
}
