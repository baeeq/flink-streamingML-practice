package ml

import java.util

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  * Created by seventeen on 2017/7/13.
  *
  */
object FTRL {

  var w = new util.HashMap[Int, Double]()
  val lamda1 = 0.1
  val lamda2 = 0.3
  val alpha = 0.03
  val beta = 1
  val dim = 100
  val z: Array[Double] = new Array[Double](dim)
  val n: Array[Double] = new Array[Double](dim)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val hostname: String = "localhost"
    val port: Int = 3000

    val trainingData = env.socketTextStream(hostname, port, '\n')

  //input stream: index, value, label

    val data: DataStream[Data] = trainingData.map(s => {
      val splits = s.split(",")
      Data(splits(0).toInt, splits(1).toDouble, splits(2).toDouble)
    })

    val model: DataStream[Params] = data
        .filter(x => x.index < dim && x.index >= 0)  // index小于0或大于维度的过滤
        .keyBy(_.index)
      .timeWindow(Time.seconds(10))
        .apply{(
          key: Int,
          window: TimeWindow,
          events: Iterable[Data],
          out: Collector[Params]) =>
          out.collect(buildPartialModel(key, events))
        }
    model.print().setParallelism(1)
    env.execute("model train")

  }


  def buildPartialModel(key: Int, value: Iterable[Data]): Params = {
    var postW = w.get(key)
    val zi = z(key)
    val ni = n(key)
    value.map{
      e => {
        val prob = predict(zi, ni, key, e, postW)
        val
        update(key, prob, value)
        res._1
      }
    }
  }

  def predict(zi: Double, ni: Double, key: Int, e: Data, postW: Double): Double = {
    var post = postW
    var wTx = 0
    val sgn = {
      if (zi < 0){
        -1
      }
      else{
        1
      }
    }
    if (sgn * zi <= lamda1){
      post = 0
    }
    else{
      post = (sgn * lamda1 - zi) / (lamda2 + (beta + Math.sqrt(ni)) / alpha)
    }
//    wTx += w.get(key) * e.value
//
//
//    w.put(key, post)
//    (Params(key, post), 1 / (1 + Math.exp(-Math.max(Math.min(wTx, 35), -35))))
  }

  def update(key: Int, prob: Double, data: Iterable[Data]): Unit = {
    var zi = 0
    var ni = 0
    data.map{ e=>
    val ans = {
      if (e.label > 0){
        1
      }
      else{
        0
      }

    }
      val g = (prob - ans) * e.value
      val sigma = ( .sqrt(n(key) + g * g) - Math.sqrt(n(key))) / alpha
      zi += g - sigma * w.get(key)
      ni += g*g
      0
  }
    z(key) += zi
    n(key) += ni
  }

  def partialGradient(x: Double, y : Double): Double = {
    y - x
  }

  case class Data(index: Int, value:Double, label: Double)

  case class Params(i: Int, w: Double) {

  }

}


