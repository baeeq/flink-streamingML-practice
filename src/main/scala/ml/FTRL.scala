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
//  var wTx = 0.0
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

    // input stream: label feature_index1: value1, feature_index2: value2, ...,

    val data: DataStream[Data] = trainingData
        .map(s => {
          var wTx = 0.0
          val splits = s.split(" ") // splits(0): label
          for (elem <- splits(1).split(',')){
            wTx = wTx + elem.split(':')(1).toDouble * w.get(elem.split(':')(0).toInt)
          }
          (splits(0).toInt, splits(1), wTx)
        }
        )
      .flatMap(x => {
        x._2.split(',').map((x._1, _, x._3))
      }).map(x => {
      val splits = x._2.split(':')
      Data(x._1, splits(1).toDouble, splits(2).toDouble, x._3)
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


/*    val data: DataStream[Data] = trainingData
      .map(s => {
      val splits = s.split(",")
      Data(splits(0).toInt, splits(1).toDouble, splits(2).toDouble, splits(3).toDouble)
    })*/

/*    val model: DataStream[Params] = data
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
    env.execute("model train")*/

  }


  def buildPartialModel(key: Int, value: Iterable[Data]): Params = {
    var postW = w.get(key)
    var prob = 0
    var zi = z(key)
    var ni = n(key)
    value.map{
      e => {
        (postW, prob) = predict(zi, ni, key, e, postW)  // 串行 or 并行？
        (zi, ni) = update(key, prob, postW, e, zi, ni)
        0
      }
    }
    w.put(key, postW)
    z(key) = zi
    n(key) = ni
    Params(key, postW)
  }

  def predict(zi: Double, ni: Double, key: Int, e: Data, postW: Double): (Double, Double) = {
    var post = postW
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
    val wTx  = e.wTx - w.get(key) * e.value + post * e.value
    (post, 1 / (1 + Math.exp(-Math.max(Math.min(wTx, 35), -35))))
  }

  def update(key: Int, prob: Double, w:Double, e: Data, zi:Double, ni:Double): (Double, Double) = {
    var zi = 0
    var ni = 0
    val ans = {
      if (e.label > 0){
        1
      }
      else{
        0
      }

    }
      val g = (prob - ans) * e.value
      val sigma = ( Math.sqrt(n(key) + g * g) - Math.sqrt(n(key))) / alpha
      zi += g - sigma * w
      ni += g*g
    (zi, ni)
  }

/*  def partialGradient(x: Double, y : Double): Double = {
    y - x
  }*/

  case class Data(index: Int, value:Double, label: Double, wTx: Double)

  case class Params(i: Int, w: Double) {

  }

}


