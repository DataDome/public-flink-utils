package co.datadome.flinkutils.util

import org.apache.flink.api.common.typeinfo.TypeInformation
import shapeless._

import scala.reflect.ClassTag


/** A hash of the structure of a type used as a Flink state. When the structure of type changes, so does the signature.
 * Used in UIDs to make sure the UID changes when the state structure changes, but normally not directly: it is used
 * through `StateTypeSignature`.
 *
 * Note: when using `fromTypeInfo` or `withTypeInfo`, you need to
 * `import org.apache.flink.streaming.api.scala.createTypeInformation` so that TypeInformations are in the implicit
 * scope.
 */
class TypeSignature[A] protected(val hashes: List[Int]) {
  lazy val hash: Int = hashes.##

  private def this(str: String) = this(str.## :: Nil)

  private def appendHash(i: Int): TypeSignature[A] = new TypeSignature[A](i :: hashes)

  private def appendString(str: String): TypeSignature[A] = appendHash(str.##)

  private def appendSignature(ts: TypeSignature[_]): TypeSignature[A] = appendHash(ts.hash)
}

object TypeSignature {

  /** Converts a TypeInformation into a TypeSignature. */
  def fromTypeInfo[A](implicit typeInfo: TypeInformation[A]): TypeSignature[A] =
    new TypeSignature[A](typeInfo.toString)

  /** Creates a StateTypeSignature for a specific enumeration. */
  def forEnum[E <: Enumeration](e: E): TypeSignature[E] = {
    val values = e.values.map(_.##).toList
    new TypeSignature[E](e.getClass.getName.## :: values)
  }

  /** It is debatable wether that method should be implicit. It would make all case class have an implicit signature
   * automatically, which might make it easier to forget to update it if we add a type inside the case class (instead of
   * as one of the arguments). */
  def forSimpleCaseClass[A: ClassTag](implicit aux: Aux[A]): TypeSignature[A] = aux.get

  def apply[A: ClassTag]: Builder[A] = Builder[A]()

  /** A Builder for the StateSignature of a type. */
  case class Builder[A] private[TypeSignature](
      private val content: TypeSignature[A] = new TypeSignature[A](Nil)
  )(implicit tag: ClassTag[A]) {

    def withType[B](implicit sign: TypeSignature[B]): Builder[A] =
      copy(content = content.appendSignature(sign))

    def withSimpleCaseClass[B](implicit aux: Aux[A]): Builder[A] =
      copy(content = content.appendSignature(aux.get))

    def withTypeInfo[B](implicit typeInfo: TypeInformation[B]): Builder[A] =
      withType(fromTypeInfo(typeInfo))

    def withEnum(es: Enumeration*): Builder[A] =
      copy(content = es.foldLeft(content) { (c, e) => c.appendSignature(forEnum(e)) })

    def build: TypeSignature[A] =
      content.appendString(tag.runtimeClass.getName)
  }

  /* A bunch of default TypeSignature, to avoid having to call fromTypeInfo everywhere */

  implicit val IntTypeSignature: TypeSignature[Int] = new TypeSignature[Int]("Int")
  implicit val BooleanTypeSignature: TypeSignature[Boolean] = new TypeSignature[Boolean]("Boolean")
  implicit val LongTypeSignature: TypeSignature[Long] = new TypeSignature[Long]("Long")
  implicit val FloatTypeSignature: TypeSignature[Float] = new TypeSignature[Float]("Float")
  implicit val DoubleTypeSignature: TypeSignature[Double] = new TypeSignature[Double]("Double")
  implicit val CharTypeSignature: TypeSignature[Char] = new TypeSignature[Char]("Char")
  implicit val ByteTypeSignature: TypeSignature[Byte] = new TypeSignature[Byte]("Byte")
  implicit val StringTypeSignature: TypeSignature[String] = new TypeSignature[String]("String")

  implicit def listTypeSignature[A](implicit aSign: TypeSignature[A]): TypeSignature[List[A]] =
    new TypeSignature[List[A]]("List").appendSignature(aSign)

  implicit def seqTypeSignature[A](implicit aSign: TypeSignature[A]): TypeSignature[Seq[A]] =
    new TypeSignature[Seq[A]]("Seq").appendSignature(aSign)

  implicit def setTypeSignature[A](implicit aSign: TypeSignature[A]): TypeSignature[Set[A]] =
    new TypeSignature[Set[A]]("Set").appendSignature(aSign)

  implicit def mapTypeSignature[K, V](implicit kSign: TypeSignature[K], vSign: TypeSignature[V]): TypeSignature[Map[K, V]] =
    new TypeSignature[Map[K, V]]("Map").appendSignature(kSign).appendSignature(vSign)

  implicit def coupleTypeSignature[A: TypeSignature, B: TypeSignature]: TypeSignature[(A, B)] =
    TypeSignature[(A, B)].withType[A].withType[B].build

  implicit def tripletTypeSignature[A: TypeSignature, B: TypeSignature, C: TypeSignature]: TypeSignature[(A, B, C)] =
    TypeSignature[(A, B, C)].withType[A].withType[B].withType[C].build

  implicit def quadrupletTypeSignature[A: TypeSignature, B: TypeSignature, C: TypeSignature, D: TypeSignature]: TypeSignature[(A, B, C, D)] =
    TypeSignature[(A, B, C, D)].withType[A].withType[B].withType[C].withType[D].build

  /* Implicit definitions for HLists, then derive those for case classes. See Shapeless documentation for more details. */

  implicit val hnilTypeSignature: TypeSignature[HNil] = TypeSignature[HNil].build

  implicit def hlistTypeSignature[H: TypeSignature, T <: HList : TypeSignature]: TypeSignature[H :: T] =
    TypeSignature[H :: T].withType[H].withType[T].build

  class Aux[A](ts: TypeSignature[A]) {
    def get: TypeSignature[A] = ts
  }

  object Aux {
    implicit def simpleCaseClassTypeSignature[A: ClassTag, R](implicit gen: Generic.Aux[A, R], rSign: TypeSignature[R]): Aux[A] =
      new Aux(TypeSignature[A].withType[R].build)
  }

}
