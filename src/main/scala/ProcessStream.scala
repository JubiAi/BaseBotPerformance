
package ai.jubi

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.sql.functions._

import scala.util.Try


object ProcessStream {

  def main(args: Array[String]):Unit=startDStream()

  private def startDStream():Unit={
    val ssc =if(Configurations.checkpoint) StreamingContext.getOrCreate(Configurations.checkpointDir, this.getContext _)else this.getContext()
    ssc.start()
    Helper.gracefulAwaitTermination(ssc)
  }

  private def dStreamOperations(stream:InputDStream[ConsumerRecord[String,String]]): Unit = {

    val rawStream:DStream[String] = stream
      .map(_.value)
      .filter(!_.isEmpty)

    val summaryStream:DStream[(String, String, String, String, String, String,String,String,String,String)]=
      rawStream
        .transform((rdd,sparkTime) => {

          Helper.printTaskDelay(s"XXXXX start - ${rdd.getNumPartitions} partitions XXXXX",sparkTime,Configurations.sessionBatchIntervalInSeconds)
          val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
          import spark.sqlContext.implicits._
          var df: DataFrame = spark.read.json(spark.createDataset(rdd))
          if (df.columns.length > 0) {
            Configurations.schemaElements.foreach(el=>{
              df=df.withColumn(el,Try(df.col(el)).toOption.getOrElse(lit("")))
            })

            df.select(Configurations.schemaElements.map(col):_*)
              .as[(String, String, String, String, String, String, String, String, String, String, String)]
              .rdd

          }
          else {
              getEmptyRDD(rdd)
          }
        })
        .transform(rdd=>{
            rdd.filter{case (projectId:String,_,_,_,_,_,_,_,_,_,humanSupport:String) =>((!Helper.checkIfPresent(humanSupport)||humanSupport.equals("false"))&&Helper.checkIfPresent(projectId))}
            .map{
              case (projectId,channel,senderId,status,prevConversationType,previousStageName,intentName,intentDetected,stageName,timestamp,_) =>   (projectId,channel,senderId,status,prevConversationType,previousStageName,intentName,intentDetected,stageName,timestamp)
            }
//              .cache()
        })

//    EventSourceTF.run(rawStream,summaryStream)
    BotAnalyticsTF.run(summaryStream)


  }

  private def getEmptyRDD(rdd:RDD[String]):RDD[(String, String, String, String, String, String,String,String,String,String,String)]={
    val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
    import spark.sqlContext.implicits._
    Seq(Configurations.schemaInit)
      .toDF(Configurations.schemaElements:_*)
      .filter(col("projectId").notEqual(""))
      .as[(String, String, String, String, String, String,String,String,String,String,String)]
      .rdd.filter(_=>false)
  }

  private def getContext(): StreamingContext = {

    val ssc = new StreamingContext(new SparkConf()
      .setAppName(Configurations.appName)
      .setAll(Configurations.sparkSettings),Seconds(Configurations.sessionBatchIntervalInSeconds))

    Helper.setupLogging()

    this.dStreamOperations(KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Configurations.topics, Configurations.kafkaParams)
    ))

    if(Configurations.checkpoint) {
      ssc.checkpoint(Configurations.checkpointDir)
    }
    ssc
  }


}

