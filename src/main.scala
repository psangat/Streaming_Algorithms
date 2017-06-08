import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable

object main {

  // Global Variables
  var firstTime = 0L
  val hbit = new mutable.HashMap[String, mutable.BitSet]
  val m1 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m2 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m3 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m4 = new mutable.HashMap[String, mutable.ListBuffer[String]]
  val m12 = new mutable.HashMap[String, Array[mutable.ListBuffer[String]]]
  val m123 = new mutable.HashMap[String, Array[mutable.ListBuffer[String]]]
  val mFix = List(m1, m2, m3, m4)
  var isFirst = true
  var y = new mutable.BitSet()
  y += 1
  var bittot = new mutable.BitSet(mFix.length)
  for (i <- mFix.indices)
    bittot += i
  var kafkaServer = "118.138.244.164:9092"

  def main(args: Array[String]): Unit = {
    // Disabling the logs of type "org" and "akka"
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (args.length != 1) {
      println("Must have parameter x || m || am")
      sys.exit()
    }

    val master = "local[*]" // At least 2 required, 1 thread used to run receiver. local[n] n > number of receivers
    val ssc = new StreamingContext(master, "Kafka Streaming", Seconds(1))
    val kafkaParams = Map("metadata.broker.list" -> kafkaServer)
    val topics1 = List("stream1").toSet
    val topics2 = List("stream2").toSet
    val topics3 = List("stream3").toSet
    val topics4 = List("stream4").toSet

    val stream1 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics1)
    val stream2 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics2)
    val stream3 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics3)
    val stream4 = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics4)

    val words1 = stream1.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))
    val words2 = stream2.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))
    val words3 = stream3.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))
    val words4 = stream4.map(_._2).map(x => (x.split(" ")(0), x.split(" ")(1)))

    words1.foreachRDD(rdd => rdd.foreach {
      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m1, 0)
      else if (args(0).equals("m")) mJoin(k, v, m1, 0)
      else if (args(0).equals("am")) amJoin(k, v, m1, 0)
    })

    words2.foreachRDD(rdd => rdd.foreach {
      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m2, 1)
      else if (args(0).equals("m")) mJoin(k, v, m2, 1)
      else if (args(0).equals("am")) amJoin(k, v, m2, 1)
    })

    words3.foreachRDD(rdd => rdd.foreach {
      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m3, 2)
      else if (args(0).equals("m")) mJoin(k, v, m3, 2)
      else if (args(0).equals("am")) amJoin(k, v, m3, 2)
    })

    words4.foreachRDD(rdd => rdd.foreach {
      case (k, v) => if (args(0).equals("x")) xJoin(k, v, m4, 3)
      else if (args(0).equals("m")) mJoin(k, v, m4, 3)
      else if (args(0).equals("am")) amJoin(k, v, m4, 3)
    })

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate
  }

  def xJoin(k: String, v: String, m: mutable.HashMap[String, mutable.ListBuffer[String]], ix: Int): Unit = {
    if (isFirst) {
      isFirst = false;
      firstTime = System.currentTimeMillis()
    }

    val startJoin = System.nanoTime()
    if (m.contains(k)) {
      m(k) += v
    }
    else {
      m(k) = mutable.ListBuffer(v)
    }
    // Insert into hash table
    val endHash = System.nanoTime()
    var isJoinable = false
    if (ix == 0) {
      // If match with m2 then hash in m12
      if (mFix(1).contains(k)) {
        if (m12.contains(k)) {
          m12(k)(ix) += v
        }
        else {
          m12(k) = Array(mutable.ListBuffer(v), mFix(1)(k))
        }
        val endHash12 = System.nanoTime()
        // If match with m3 then hash in m123
        if (mFix(2).contains(k)) {
          if (m123.contains(k)) {
            m123(k)(ix) += v
          }
          else {
            m123(k) = Array(mutable.ListBuffer(v), m12(k)(1), mFix(2)(k))
          }
          if (mFix(3).contains(k)) {
            isJoinable = true
          }
        }
      }
    } else if (ix == 1) {
      // If match with m1 then hash in m12
      if (mFix(0).contains(k)) {
        if (m12.contains(k)) {
          m12(k)(ix) += v
        }
        else {
          m12(k) = Array(mFix(0)(k), mutable.ListBuffer(v))
        }
        val endHash12 = System.nanoTime()
        // If match with m3 then hash in m123
        if (mFix(2).contains(k)) {
          if (m123.contains(k)) {
            m123(k)(ix) += v
          }
          else {
            m123(k) = Array(m12(k)(0), mutable.ListBuffer(v), mFix(2)(k))
          }
          if (mFix(3).contains(k)) {
            isJoinable = true
          }
        }
      }
    } else if (ix == 2) {
      // If match with m12 then hash in m123
      if (m12.contains(k)) {
        if (m123.contains(k)) {
          m123(k)(ix) += v
        }
        else {
          m123(k) = Array(m12(k)(0), m12(k)(1), mutable.ListBuffer(v))
        }
        if (mFix(3).contains(k)) {
          isJoinable = true
        }
      }
    } else {
      // Join with m12
      if (m123.contains(k)) {
        isJoinable = true
      }
    }
    println((System.nanoTime() - startJoin) + " " + (endHash - startJoin) + " " + isJoinable + " " + k + " " + ix + " " + (System.currentTimeMillis() - firstTime))
  }

  def mJoin(k: String, v: String, m: mutable.HashMap[String, mutable.ListBuffer[String]], ix: Int): Unit = {
    if (isFirst) {
      isFirst = false
      firstTime = System.currentTimeMillis()
    }
    val startJoin = System.nanoTime()
    if (m.contains(k)) {
      m(k) += v
    }
    else {
      m(k) = mutable.ListBuffer(v)
    }
    //Insert Into Hash Table
    val endHash = System.nanoTime()
    val tmpm = new mutable.HashMap[Int, mutable.ListBuffer[String]]
    var i = 0
    var isJoinable = true
    while (i < mFix.length && isJoinable) {
      if (i != ix) {
        if (mFix(i).contains(k)) tmpm(i) = mFix(i)(k) else isJoinable = false
      } else {
        tmpm(ix) = mutable.ListBuffer(v)
      }
      i = i + 1
    }
    println((System.nanoTime() - startJoin) + " " + (endHash - startJoin) + " " + isJoinable + " " + k + " " + ix + " " + (System.currentTimeMillis() - firstTime))
  }

  def amJoin(k: String, v: String, m: mutable.HashMap[String, mutable.ListBuffer[String]], ix: Int): Unit = {
    if (isFirst) {
      isFirst = false;
      firstTime = System.currentTimeMillis()
    }

    val startJoin = System.nanoTime()
    if (m.contains(k)) {
      m(k) += v
    }
    else {
      m(k) = mutable.ListBuffer(v)
    }
    //Insert into hash table
    val endHash = System.nanoTime()
    var isJoinable = true
    var endByBV = 1
    if (hbit.contains(k)) {
      hbit(k) += ix
      if ((hbit(k) & bittot).size != 3) isJoinable = false
    } else {
      hbit(k) = mutable.BitSet()
      hbit(k) += ix
      isJoinable = false
    }
    if (isJoinable) {
      endByBV = 0
      val tmpm = new mutable.HashMap[Int, mutable.ListBuffer[String]]
      var i = 0
      while (i < mFix.length && isJoinable) {
        if (i != ix) {
          if (mFix(i).contains(k)) tmpm(i) = mFix(i)(k) else isJoinable = false
        } else {
          tmpm(ix) = mutable.ListBuffer(v)
        }
        i = i + 1
      }
    }
    println((System.nanoTime() - startJoin) + " " + (endHash - startJoin)
      + " " + endByBV + " " + isJoinable + " " + k + " " + ix + " " + (System.currentTimeMillis() - firstTime))
  }
}
