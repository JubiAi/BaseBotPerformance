
package ai.jubi



import com.mongodb.spark._
import com.mongodb.spark.config._
import org.bson.Document
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

object Sink {



  def run(rdd:RDD[String],collection:String,sparkTimestamp:String): Unit ={
    if(Configurations.printOnly){
      println(rdd.count(),collection)
    }
    else {
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import KafkaRDDSink._
      import spark.implicits._
      rdd.sendToKafka(Configurations.kafkaProduceUri, collection)
      rdd.map(Document.parse).saveToMongoDB(WriteConfig(Map("uri" -> Configurations.dbUri(collection))))
    }
  }

  def runWithoutKafka(rdd:RDD[String],collection:String,sparkTimestamp:String): Unit ={
    if(Configurations.printOnly){
      println(rdd.count(),collection)
    }
    else {
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import spark.implicits._
      rdd.map(Document.parse).saveToMongoDB(WriteConfig(Map("uri" -> Configurations.dbUri(collection))))
      rdd.toDF().as[String].write.mode(SaveMode.Append).text(Configurations.storageUri(collection, sparkTimestamp))
    }
  }

  def runWithPartitions(rdd:RDD[String],collection:String,sparkTimestamp:String): Unit ={
    if(Configurations.printOnly){
      println(rdd.count(),collection)
    }
    else {
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      import KafkaRDDSink._
      import spark.implicits._
      rdd.sendToKafkaWithPartitions(Configurations.kafkaProduceUri, collection)
      rdd.map(Document.parse).saveToMongoDB(WriteConfig(Map("uri" -> Configurations.dbUri(collection))))
    }
  }


}

