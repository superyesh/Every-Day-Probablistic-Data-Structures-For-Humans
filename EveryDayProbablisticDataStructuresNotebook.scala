// Databricks notebook source
case class Event(
  productId: String,
  eventType : String, //PageVisit | AddToCart | Purchase
  userId : String,
  totalPrice : Double,
  sellerId : String,
  ipAddress : String
)

// COMMAND ----------

val seqEvent = Seq(
Event("p1", "addToCart", "user1", 3.0, "seller1", "1.0.0.1"),
Event("p2", "addToCart", "user2", 3.0, "seller2", "1.0.0.2"),
Event("p2", "purchase", "user2", 3.0, "seller2", "1.0.0.2"),
Event("p3", "visit", "user3", 3.0, "seller1", "1.0.0.3"),
Event("p4", "purchase", "user1", 6.0, "seller1", "1.0.0.4"),
Event("p5", "purchase", "user2", 10.0, "seller2", "1.0.0.1")
)

val eventDF = sc.parallelize(seqEvent).toDF
val eventSchema = eventDF.schema

// COMMAND ----------

val r = scala.util.Random
val buf = scala.collection.mutable.ListBuffer.empty[Event]
for( x <- 1 to 100000){
  val i = r.nextInt(2)
  val eventType= Seq("addToCart", "purchase", "visit")
  buf+= Event(s"product${r.nextInt(10000)}", eventType(i), s"user${r.nextInt(50000)}", r.nextInt(100), s"seller${r.nextInt(2000)}", s"10.0.${r.nextInt(128)}.${r.nextInt(128)}")
}
val eventDF = sc.parallelize(buf.toSeq).toDF
val eventSchema = eventDF.schema
eventDF.write.mode("overwrite").parquet("dbfs:/FileStore/event.parquet")
val eventStream = spark.readStream.schema(eventSchema).parquet("dbfs:/FileStore/event.parquet")

// COMMAND ----------

import java.util.Properties

import com.adobe.aep.unifiedprofile.common.env.EnvConfig
import com.adobe.aep.unifiedprofile.previewjob.RedisConfig
import org.apache.spark.sql.SparkSession
import com.adobe.aep.unifiedprofile.utils.JsonUtils
import play.api.libs.json._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import io.lettuce.core._
import kafkashaded.org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.spark.sql.{ForeachWriter, Row}

import com.adobe.aep.unifiedprofile.common.metrics.{MetricTag, MetricsGlobals, WarpMetricReporter}
import com.adobe.platform.data.malt.api.Tag

import scala.util.Try
import org.apache.spark.sql.expressions.scalalang.typed

import java.sql.Timestamp
import java.util.Date
import java.text.SimpleDateFormat
val redisConf = RedisConfig(
   REDIS_HOST,
    "6380",
    REDIS_AUTH
  )

def getRedisConnection() ={
    @transient lazy val redisConnection = RedisClient.create(RedisURI.builder().withHost(redisConf.redisHost).withDatabase(3).withPort(redisConf.redisPort.toInt).withPassword(redisConf.redisAuth).withSsl(true).build()).connect
  redisConnection
}

// COMMAND ----------

import com.twitter.algebird.HyperLogLog
import com.twitter.algebird.HyperLogLogMonoid
import com.twitter.algebird._
import org.apache.commons.codec.binary.Base64
import com.twitter.chill.algebird.AlgebirdRegistrar
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}

import com.twitter.chill.algebird.AlgebirdRegistrar
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}
import  org.apache.spark.util.sketch.BloomFilter


object KryoEnv {
  val KryoPoolSize = 10

  val kryo = {
    val inst = () ⇒ {
      val newK = (new ScalaKryoInstantiator).newKryo()
      newK.setReferences(false)
      (new AlgebirdRegistrar).apply(newK)
      newK
    }
    KryoPool.withByteArrayOutputStream(KryoPoolSize, inst)
  }

  def decodeHLL(hllHash:String): com.twitter.algebird.HLL = {
    // decode the string into bytes
    val bytes = Base64.decodeBase64(hllHash)
    // hhh is a Hyperloglog data structure
    val hyperll = kryo.fromBytes(bytes).asInstanceOf[HLL]
    // return hyperll
    hyperll
  }

  def encodeHLL(hll: HLL): String = {
    // takes hll and converts to bytes
    val bytes: Array[Byte] = kryo.toBytesWithClass(hll)
    // encode to Base64
    val encoded = Base64.encodeBase64(bytes)
    // create String object as Base64 encoded HLL
    new String(encoded)
  }
//   def decodetoBF(bfHash:String): BF[String] = {
//     // decode the string into bytes
//     val bytes = Base64.decodeBase64(bfHash)
//     // hhh is a Hyperloglog data structure
//     val bfll = kryo.fromBytes(bytes).asInstanceOf[BF[String]]
//     // return hyperll
//     bfll
//   }

//   def encodeBF(bf: BF[String]): String = {
//     // takes hll and converts to bytes
//     val bytes: Array[Byte] = kryo.toBytesWithClass(bf)
//     // encode to Base64
//     val encoded = Base64.encodeBase64(bytes)
//     // create String object as Base64 encoded HLL
//     new String(encoded)
//   }
  
  def decodetoBF(bfHash:String): org.apache.spark.util.sketch.BloomFilter = {
    // decode the string into bytes
    val bytes = Base64.decodeBase64(bfHash)
    // hhh is a Hyperloglog data structure
    val bfll = kryo.fromBytes(bytes).asInstanceOf[org.apache.spark.util.sketch.BloomFilter]
    // return hyperll
    bfll
  }

  def encodeBF(bf: org.apache.spark.util.sketch.BloomFilter): String = {
    // takes hll and converts to bytes
    val bytes: Array[Byte] = kryo.toBytesWithClass(bf)
    // encode to Base64
    val encoded = Base64.encodeBase64(bytes)
    // create String object as Base64 encoded HLL
    new String(encoded)
  }


}

// COMMAND ----------

// eventDF.write.mode("overwrite").parquet("dbfs:/FileStore/event.parquet")

// COMMAND ----------

val eventDF = spark.read.parquet("dbfs:/FileStore/event.parquet")
val eventStream = spark.readStream.schema(eventDF.schema).parquet("dbfs:/FileStore/event.parquet")

// COMMAND ----------

// Helper Method to Update Redis with Bloom Filter
def updateBloomFilter(connection: io.lettuce.core.api.StatefulRedisConnection[String,String] ,key: String, newBF:org.apache.spark.util.sketch.BloomFilter) = {
  val existing = Option(connection.sync.get(key))
  existing match {
    case Some(x: String) => {
      //  Bloom Filter Exists, fetch existing + merge and update
      val existingBF = KryoEnv.decodetoBF(x)
      existingBF.mergeInPlace(existingBF)
      connection.sync.set(key, KryoEnv.encodeBF(existingBF))      
    }
    case None=>{
      // Bloom Filter doesnt exist for key
      connection.sync.set(key, KryoEnv.encodeBF(newBF))
    }
  }
}

// COMMAND ----------

// Helper Method to fetch the Bloom Filter from Redis

def getBFforProduct(connection: io.lettuce.core.api.StatefulRedisConnection[String,String] ,product: String) = {
  val existing = Option(connection.sync.get(s"$product-userSet-bloomFilter"))
  existing match {
    case Some(x: String) => {
      val existingBF = KryoEnv.decodetoBF(x)
      existingBF    
    }
    case None=>{
      org.apache.spark.util.sketch.BloomFilter.create(10000)
    }
  }
}

// COMMAND ----------

// Create the Bloom Filters and Update them

val bfQuery = eventStream.writeStream.foreachBatch {
        (batchDF: DataFrame, batchId: Long) => {
        @transient val redis = getRedisConnection
        val uniqueProductsinBatch = batchDF.select($"productId").distinct.collect
        println(uniqueProductsinBatch)
        uniqueProductsinBatch.foreach{
          row => {
            val productId = row.getAs[String](0)
            val productDF = batchDF.filter($"productId" === productId)
            val bloomFilterForProduct = productDF.stat.bloomFilter($"userId", 10000, 0.01)
            updateBloomFilter(connection=redis, key=s"$productId-userSet-bloomFilter", newBF=bloomFilterForProduct)                        
          }
        }
        redis.close()
       }
        
}
bfQuery.start

// COMMAND ----------

val r = getRedisConnection
val product_bloomFilter = KryoEnv.decodetoBF(r.sync.get("product8788-userSet-bloomFilter"))

// COMMAND ----------

val r = getRedisConnection

r.sync.keys("product*")
// product_bloomFilter.mightContain("user27323")

// COMMAND ----------

getBFforProduct(r, "product8788").mightContain("user24500")

// COMMAND ----------

display(eventDF.filter($"productId" === "product8788"))

// COMMAND ----------

// MAGIC %md HLL Processing

// COMMAND ----------



// COMMAND ----------

// Create the HLL  and Update them
// With a twist - use redis fully!

@transient val hllQuery = eventStream.writeStream.foreachBatch {
        (batchDF: DataFrame, batchId: Long) => {
        @transient val groupedProductDF = batchDF.filter($"eventType" === "purchase").groupBy($"productId").agg(array_distinct(collect_list($"userId")).alias("users"))
        groupedProductDF.foreach{
          row=> {
            @transient val redis = getRedisConnection
            val productId = row.getString(0)
            val users = row.getAs[Seq[String]]("users")            
            redis.sync.pfadd(s"$productId-purchase-hll", users:_*)
            redis.close()
          }
        }     
       }        
}


// COMMAND ----------

hllQuery.start

// COMMAND ----------



// COMMAND ----------

val fullPurchaseDF = eventDF.filter($"eventType" === "purchase").groupBy($"productId").agg(array_distinct(collect_list($"userId")).alias("users"))
display(fullPurchaseDF)

// COMMAND ----------

// Get count of users who bought product X
r.sync.pfcount("product1619-purchase-hll")

// COMMAND ----------

// Get count of users who bought product X, Y and Z
r.sync.pfcount("product1619-purchase-hll", "product2109-purchase-hll",  "product3661-purchase-hll")

// COMMAND ----------

display(fullPurchaseDF.filter($"productId" isin ("product1619", "product2109", "product3661" )))

// COMMAND ----------


