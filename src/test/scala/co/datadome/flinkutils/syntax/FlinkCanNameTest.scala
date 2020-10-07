package co.datadome.flinkutils.syntax

import co.datadome.flinkutils.util.{StateTypeSignature, TypeSignature}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FlinkCanNameTest extends AnyFunSuite with Matchers {

  import FlinkCanNameTest._

  private val env = StreamExecutionEnvironment.getExecutionEnvironment

  test("DataStream.uidName") {
    val stream: DataStream[String] = env.fromElements("").uidName("test")
    stream.getUid should be("test")
    stream.name should be("test")
  }

  test("DataSink.uidName") {
    val stream = env.fromElements("")
    val sink = stream.addSink(new DiscardingSink[String]).uidName("test")
    sink.getUid should be("test")
    sink.getName should be("test")
  }

  test("DataStream.uidNameStated > same UID for the same class") {
    val stream1 = env.fromElements("").uidNameStated[MyProcessFunction[Long]]("test")
    val stream2 = env.fromElements("").uidNameStated[MyProcessFunction[Long]]("test")

    stream1.getUid should be(stream2.getUid)
  }

  test("DataStream.uidNameStated > different UIDs for different classes") {
    val stream1 = env.fromElements("").uidNameStated[MyProcessFunction[Long]]("test")
    val stream2 = env.fromElements("").uidNameStated[MyProcessFunction[String]]("test")

    stream1.getUid shouldNot be(stream2.getUid)
  }

}


object FlinkCanNameTest {

  class MyProcessFunction[A] extends ProcessFunction[A, A] {
    override def processElement(value: A, ctx: ProcessFunction[A, A]#Context, out: Collector[A]): Unit =
      out.collect(value)
  }

  object MyProcessFunction {
    implicit def stateTypeSignature[A: TypeSignature]: StateTypeSignature[MyProcessFunction[A]] =
      StateTypeSignature.forUnkeyedFunction[MyProcessFunction[A]].withValueState[A].build
  }

  private implicit class StreamOps(val wrapped: DataStream[_]) extends AnyVal {
    def getUid: String = wrapped.javaStream.getTransformation.getUid
  }

  private implicit class SinkOps(val wrapped: DataStreamSink[_]) extends AnyVal {
    def getUid: String = wrapped.getTransformation.getUid

    def getName: String = wrapped.getTransformation.getName
  }

}
