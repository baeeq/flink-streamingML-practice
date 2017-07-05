package ml

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * Created by seventeen on 2017/7/2.
  *
  * Window batch incremental SGD
  *
  * 2017.07.04
  * Done：
  * 1. y = wx + b 的无正则化SGD求解
  * 2. 以窗口为微批迭代计算，迭代次数固定
  * 3. 并行度为1
  *
  * TODO:
  * 1. 判断收敛条件：损失函数
  * 2. 加入正则化项
  * 3. 扩展到n维
  * 4. 并行化：
  * BSP
  *           a. 通过keyBy()并行;
  *           b. 如何迭代;
  *           c. 如何同步参数.
  * SSP
  *           a. 数据形式;
  *           b. 如何分区;
  *           c. 如何迭代;
  *           d. 如何同步参数.
  *
  */
object IncrementalMiniBatchSGD {
  var postModel = Params(0.0, 0.0)

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val hostname: String = "localhost"
    val port: Int = 3000

    val trainingData = env.socketTextStream(hostname, port, '\n')

    val data: DataStream[Data] = trainingData.map(x => {
      val splits = x.split(',')
      Data(splits(0).toDouble, splits(1).toDouble)
    })

    val model: DataStream[Params] = data
//        .keyBy()
      .timeWindowAll(Time.seconds(30))
      .apply { (
                 window: TimeWindow,
                 events: Iterable[Data],
                 out: Collector[Params]) =>
        out.collect(buildPartialModel(events))
      }

    model.print().setParallelism(1)
    env.execute("model train")

  }

  def buildPartialModel(value: Iterable[Data]): Params = {


    var i = 0
    var prev = postModel
    while (i < 10) {
      i += 1
      val cur = value.
        map(data => {
          val theta0 = prev.theta0 - 0.01 * ((prev.theta0 + (prev.theta1 * data.x)) - data.y)
          val theta1 = prev.theta0 - 0.01 * ((prev.theta0 + (prev.theta1 * data.x)) - data.y) * data.x
          (Params(theta0, theta1), 1)
        })
        .reduce { (p1, p2) =>
          val result = p1._1 + p2._1
          (result, p1._2 + p2._2)
        }
      prev = cur._1.div(cur._2)

    }
    postModel = prev
    postModel
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


