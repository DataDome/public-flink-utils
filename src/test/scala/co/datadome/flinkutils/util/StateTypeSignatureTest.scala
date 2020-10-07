package co.datadome.flinkutils.util

import org.apache.flink.api.common.state.{ListStateDescriptor, MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, KeyedBroadcastProcessFunction}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

// scalastyle:off magic.number
class StateTypeSignatureTest extends AnyFunSuite with Matchers {

  import StateTypeSignatureTest._

  test("unsafeFromTypeInfo") {
    val upf = StateTypeSignature.unsafeFromTypeInfo[MyUnkeyedProcessFunction]
    val kpf = StateTypeSignature.unsafeFromTypeInfo[MyKeyedProcessFunction]
    kpf.hash shouldNot be(upf.hash)
    kpf.hash should be(-98780014)
  }

  test("forUnkeyedFunction") {
    val s = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].noState.build
    s.hash should be(1116701602)
  }

  test("forKeyedFunction") {
    val s = StateTypeSignature.forKeyedFunction[MyKeyedProcessFunction].withKey[Int].noState.build
    s.hash should be(1169682230)
  }

  test("forBroadcastFunction") {
    val s = StateTypeSignature.forUnkeyedBroadcastFunction[MyBroadcastProcessFunction].noState.noBroadcastState.build
    s.hash should be(1318288024)
  }

  test("forKeyedBroadcastFunction") {
    val s = StateTypeSignature.forKeyedBroadcastFunction[MyKeyedBroadcastProcessFunction].withKey[Int].noState.noBroadcastState.build
    s.hash should be(2045861194)
  }

  test("withKey") {
    val s1 = StateTypeSignature.forKeyedFunction[MyKeyedProcessFunction].withKey[Int].noState.build.hash
    val s2 = StateTypeSignature.forKeyedFunction[MyKeyedProcessFunction].withKey[Int].noState.build.hash
    val s3 = StateTypeSignature.forKeyedFunction[MyKeyedProcessFunction].withKey[String].noState.build.hash
    s1 should be(s2)
    s1 shouldNot be(s3)
  }

  test("withState") {
    val ns1 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].noState.build.hash
    val ns2 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].noState.build.hash
    ns1 should be(ns2)

    val vs1 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withValueState[String].build.hash
    val vs2 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withValueState[String].build.hash
    val vs3 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withValueState[Int].build.hash
    vs1 should be(vs2)
    vs1 shouldNot be(vs3)
    vs1 shouldNot be(ns1)

    val ls1 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withListState[String].build.hash
    val ls2 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withListState[String].build.hash
    val ls3 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withListState[Int].build.hash
    ls1 should be(ls2)
    ls1 shouldNot be(ls3)
    ls1 shouldNot be(ns1)
    ls1 shouldNot be(vs1)

    val ms1 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withMapState[String, Long].build.hash
    val ms2 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withMapState[String, Long].build.hash
    val ms3 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withMapState[Int, Long].build.hash
    ms1 should be(ms2)
    ms1 shouldNot be(ms3)
    ms1 shouldNot be(ns1)
    ms1 shouldNot be(vs1)
    ms1 shouldNot be(ls1)
  }

  test("withStateDescriptor") {
    val vs1 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withValueState[String].build.hash
    val vs2 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withStateDescriptor(new ValueStateDescriptor("Toto", classOf[String])).build.hash
    vs1 should be(vs2)

    val ls1 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withListState[String].build.hash
    val ls2 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withStateDescriptor(new ListStateDescriptor("Toto", classOf[String])).build.hash
    ls1 should be(ls2)

    val ms1 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withMapState[String, Long].build.hash
    val ms2 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].withStateDescriptor(new MapStateDescriptor[String, Long]("Toto", classOf[String], classOf[Long])).build.hash
    ms1 should be(ms2)
  }

  test("withBroadcastState") {
    val ns1 = StateTypeSignature.forUnkeyedBroadcastFunction[MyBroadcastProcessFunction].noState.noBroadcastState.build.hash
    val ns2 = StateTypeSignature.forUnkeyedBroadcastFunction[MyBroadcastProcessFunction].noState.noBroadcastState.build.hash
    ns1 should be(ns2)

    val ms1 = StateTypeSignature.forUnkeyedBroadcastFunction[MyBroadcastProcessFunction].noState.withBroadcastState[String, Long].build.hash
    val ms2 = StateTypeSignature.forUnkeyedBroadcastFunction[MyBroadcastProcessFunction].noState.withBroadcastState[String, Long].build.hash
    val ms3 = StateTypeSignature.forUnkeyedBroadcastFunction[MyBroadcastProcessFunction].noState.withBroadcastState[Int, Long].build.hash
    ms1 should be(ms2)
    ms1 shouldNot be(ms3)
    ms1 shouldNot be(ns1)
  }

  test("withBroadcastStateDescriptor") {
    val vs1 = StateTypeSignature.forUnkeyedBroadcastFunction[MyBroadcastProcessFunction].noState.withBroadcastState[String, Long].build.hash
    val vs2 = StateTypeSignature.forUnkeyedBroadcastFunction[MyBroadcastProcessFunction].noState.withBroadcastStateDescriptor(new MapStateDescriptor[String, Long]("Toto", classOf[String], classOf[Long])).build.hash
    vs1 should be(vs2)
  }

  test("withType") {
    val s1 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].noState.build.hash
    val s2 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].noState.withType[String].build.hash
    val s3 = StateTypeSignature.forUnkeyedFunction[MyUnkeyedProcessFunction].noState.withType[Int].build.hash
    s1 shouldNot be(s2)
    s1 shouldNot be(s3)
    s2 shouldNot be(s3)
  }

}



object StateTypeSignatureTest {

  class MyUnkeyedProcessFunction extends ProcessFunction[String, Int] {
    override def processElement(value: String, ctx: ProcessFunction[String, Int]#Context, out: Collector[Int]): Unit = ()
  }

  class MyKeyedProcessFunction extends KeyedProcessFunction[Int, String, Int] {
    override def processElement(value: String, ctx: KeyedProcessFunction[Int, String, Int]#Context, out: Collector[Int]): Unit = ()
  }

  class MyBroadcastProcessFunction extends BroadcastProcessFunction[String, Boolean, Int] {
    override def processElement(value: String, ctx: BroadcastProcessFunction[String, Boolean, Int]#ReadOnlyContext, out: Collector[Int]): Unit = ()

    override def processBroadcastElement(value: Boolean, ctx: BroadcastProcessFunction[String, Boolean, Int]#Context, out: Collector[Int]): Unit = ()
  }

  class MyKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[Int, String, Boolean, Int] {
    override def processElement(value: String, ctx: KeyedBroadcastProcessFunction[Int, String, Boolean, Int]#ReadOnlyContext, out: Collector[Int]): Unit = ()

    override def processBroadcastElement(value: Boolean, ctx: KeyedBroadcastProcessFunction[Int, String, Boolean, Int]#Context, out: Collector[Int]): Unit = ()
  }

}
