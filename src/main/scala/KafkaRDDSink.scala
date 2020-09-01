
package ai.jubi
import java.util.Properties

import org.apache.kafka.clients.producer.Callback
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata

import scala.collection.mutable
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.json4s.{DefaultFormats, jackson}



object KafkaRDDSink {
  implicit def createKafkaRDDSink(rdd: RDD[String]): KafkaRDDSink = {
    new KafkaRDDSink(rdd)
  }
}


object KafkaProducerFactory {
  private val Producers = mutable.Map[Properties, KafkaProducer[String,String]]()
  def getOrCreateProducer(kafkaProduceUri: String): KafkaProducer[String, String] = {
    implicit val serialization = jackson.Serialization
    implicit val formats = DefaultFormats
    val serializer = "org.apache.kafka.common.serialization.StringSerializer"
    val props = new Properties()
    props.put("bootstrap.servers",kafkaProduceUri)
    props.put("key.serializer", serializer)
    props.put("value.serializer", serializer)
    Producers.getOrElseUpdate(
      props, {
        val producer = new KafkaProducer[String,String](props)
        sys.addShutdownHook {
          producer.close()
        }
        producer
      })
  }
}

class KafkaRDDSink(@transient private val rdd: RDD[String]) extends Serializable {

  def sendToKafka(kafkaProduceUri: String, topic: String): Unit = {
      rdd.foreachPartitionAsync { records =>
        val producer = KafkaProducerFactory.getOrCreateProducer(kafkaProduceUri)
        val callback = new KafkaRDDSinkExceptionHandler
        val metadata = records.map { message =>
          callback.throwExceptionIfAny()
          val jsonMessage = message.replace("\\","")
          producer.send(new ProducerRecord[String, String](topic,  jsonMessage), callback)
        }.toList
        metadata.foreach { metadata => metadata.get() }
        callback.throwExceptionIfAny()
      }
  }

  def sendToKafkaWithPartitions(kafkaProduceUri: String, topic: String): Unit = {
    rdd.mapPartitionsWithIndex((index,itr)=>itr.map((index,_))).foreachPartitionAsync { records =>
      val producer = KafkaProducerFactory.getOrCreateProducer(kafkaProduceUri)
      val callback = new KafkaRDDSinkExceptionHandler
      val metadata = records.map { case (partition,message) =>
        callback.throwExceptionIfAny()
        val jsonMessage = message.replace("\\","")
        producer.send(new ProducerRecord[String, String](topic,partition,topic,jsonMessage), callback)
      }.toList
      metadata.foreach { metadata => metadata.get() }
      callback.throwExceptionIfAny()
    }
  }


}

class KafkaRDDSinkExceptionHandler extends Callback {

  import java.util.concurrent.atomic.AtomicReference

  private val lastException = new AtomicReference[Option[Exception]](None)

  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
    Option(exception).foreach { ex => lastException.set(Some(ex)) }

  def throwExceptionIfAny(): Unit = lastException.getAndSet(None).foreach(ex => throw ex)

}