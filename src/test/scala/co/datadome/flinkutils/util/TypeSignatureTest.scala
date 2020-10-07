package co.datadome.flinkutils.util

import org.apache.flink.streaming.api.scala._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

// scalastyle:off magic.number
class TypeSignatureTest extends AnyFunSuite with Matchers {

  import TypeSignatureTest._

  test("fromTypeInfo") {
    val bb = TypeSignature.fromTypeInfo[BotBuster]
    val bk = TypeSignature.fromTypeInfo[BotKiller]
    bk.hash shouldNot be(bb.hash)
    bk.hash should be(-1527952663)
  }

  test("forEnum") {
    val f = TypeSignature.forEnum(Fruits)
    val v = TypeSignature.forEnum(Vegetables)
    f.hash shouldNot be(v.hash)
    f.hash should be(-1311016316)
  }

  test("withType") {
    val tsWithoutType = TypeSignature[TypeSignatureTest].build
    val tsWithType = TypeSignature[TypeSignatureTest].withType[BotBuster].build
    tsWithType.hash shouldNot be(tsWithoutType.hash)
    tsWithType.hash should be(4399548)
  }

  test("withTypeInfo") {
    val tsWithoutType = TypeSignature[TypeSignatureTest].build
    val tsWithType = TypeSignature[TypeSignatureTest].withTypeInfo[BotBuster].build
    tsWithType.hash shouldNot be(tsWithoutType.hash)
    tsWithType.hash should be(-1468632402)
  }

  test("withEnum") {
    val tsWithoutEnum = TypeSignature[TypeSignatureTest].build
    val tsWithEnum = TypeSignature[TypeSignatureTest].withEnum(Fruits).build
    tsWithEnum.hash shouldNot be(tsWithoutEnum.hash)
    tsWithEnum.hash should be(-1239478630)
  }

  test("withSimpleCaseClass") {
    val tsWithoutEnum = TypeSignature[TypeSignatureTest].build
    val tsWithEnum = TypeSignature[TypeSignatureTest].withSimpleCaseClass[BotKiller].build
    tsWithEnum.hash shouldNot be(tsWithoutEnum.hash)
    tsWithEnum.hash should be(397787972)
  }

  test("List implicit derivation") {
    val s = implicitly[TypeSignature[List[BotBuster]]]
    val r = implicitly[TypeSignature[BotBuster]]
    s.hash shouldNot be(r.hash)
    s.hash should be(-1597108772)
  }

  test("Seq implicit derivation") {
    val s = implicitly[TypeSignature[Seq[BotBuster]]]
    val r = implicitly[TypeSignature[BotBuster]]
    s.hash shouldNot be(r.hash)
    s.hash should be(-1811399253)
  }

  test("Set implicit derivation") {
    val s = implicitly[TypeSignature[Set[BotBuster]]]
    val r = implicitly[TypeSignature[BotBuster]]
    s.hash shouldNot be(r.hash)
    s.hash should be(1183770364)
  }

  test("Map implicit derivation") {
    val s = implicitly[TypeSignature[Map[String, BotBuster]]]
    val r = implicitly[TypeSignature[BotBuster]]
    val q = implicitly[TypeSignature[String]]
    s.hash shouldNot be(r.hash)
    s.hash shouldNot be(q.hash)
    s.hash should be(-1493981055)
  }

}

object TypeSignatureTest {

  case class BotBuster(name: String)

  object BotBuster {
    implicit val typeSignature: TypeSignature[BotBuster] = TypeSignature.forSimpleCaseClass[BotBuster]
  }

  case class BotKiller(id: Long)

  object Fruits extends Enumeration {
    val Apple, Banana = Value
  }

  object Vegetables extends Enumeration {
    val Carrot, Bean = Value
  }

}
