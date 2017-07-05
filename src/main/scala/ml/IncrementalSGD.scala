package ml

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * Created by seventeen on 2017/7/2.
  * Incremental SGD
  * TODO
  */
object IncrementalSGD {
  var postModel = Params(0.0, 0.0)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val hostname: String = "localhost"
    val port: Int = 3000

    val trainingData = env.socketTextStream(hostname, port, '\n')

    val data: DataStream[Data] = trainingData.map(x => {
      val splits = x.split(',')
      Data(splits(0).toDouble, splits(1).toDouble)
    }).keyBy(1).shuffle

    val model: DataStream[Params] = data
      .map(data => {
        val theta0 = postModel.theta0 - 0.01 * ((postModel.theta0 + (postModel.theta1 * data.x)) - data.y)
        val theta1 = postModel.theta0 - 0.01 * ((postModel.theta0 + (postModel.theta1 * data.x)) - data.y) * data.x
        (Params(theta0, theta1), 1)
      })
      .keyBy(_._1)
      .timeWindow(Time.seconds(30))
      .reduce { (p1, p2) =>
        val result = p1._1 + p2._1
        (result, p1._2 + p2._2)
      }.map { x =>
      postModel = x._1.div(x._2)
      postModel
    }


    model.print().setParallelism(1)
    env.execute("model train")

  }

  case class Data(var x: Double, var y: Double)

  case class Params(theta0: Double, theta1: Double) {
    def div(a: Int): Params = {
      Params(theta0 / a, theta1 / a)
    }

    def +(other: Params) = {
      Params(theta0 + other.theta0, theta1 + other.theta1)
    }
  }

}


