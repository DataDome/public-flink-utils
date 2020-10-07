package co.datadome.flinkutils.util

import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, CoProcessFunction, KeyedBroadcastProcessFunction, KeyedCoProcessFunction}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}

import scala.reflect.ClassTag


/** Specific kind of type signature used for Flink functions. Unlike the parent one, it cannot be deduced from
 * type-information and must be defined manually. It is used to make sure the UID changes when the state structure
 * changes. */
class StateTypeSignature[A] private(hashes: List[Int]) extends TypeSignature[A](hashes) {

  private def this(str: String) = this(str.## :: Nil)

  private def appendHash(i: Int*): StateTypeSignature[A] = new StateTypeSignature[A](i.reverse.toList ::: hashes)

  private def appendString(str: String*): StateTypeSignature[A] = appendHash(str.map(_.##): _*)

  private def appendSignature(ts: TypeSignature[_]*): StateTypeSignature[A] = appendHash(ts.map(_.hash): _*)
}

object StateTypeSignature {

  import Builder._

  /** Converts a TypeInformation into a StateTypeSignature. Should be avoided, as it doesn't always provide a signature
   * that will change when the type structure changes. */
  def unsafeFromTypeInfo[A](implicit typeInfo: TypeInformation[A]): StateTypeSignature[A] =
    new StateTypeSignature[A](typeInfo.toString)

  /** Starts the creation of a StateTypeSignature for an unkeyed function, with an empty builder. */
  def forUnkeyedFunction[A <: ProcessFunction[_, _] : ClassTag]: Builder[A, Ok, Ko, Ok] = Builder[A, Ok, Ko, Ok]()

  /** Starts the creation of a StateTypeSignature for a keyed function, with an empty builder. */
  def forKeyedFunction[A <: KeyedProcessFunction[_, _, _] : ClassTag]: Builder[A, Ko, Ko, Ok] = Builder[A, Ko, Ko, Ok]()

  /** Starts the creation of a StateTypeSignature for an unkeyed broadcast function, with an empty builder. */
  def forUnkeyedBroadcastFunction[A <: BroadcastProcessFunction[_, _, _] : ClassTag]: Builder[A, Ok, Ko, Ko] = Builder[A, Ok, Ko, Ko]()

  /** Starts the creation of a StateTypeSignature for a keyed broadcast function, with an empty builder. */
  def forKeyedBroadcastFunction[A <: KeyedBroadcastProcessFunction[_, _, _, _] : ClassTag]: Builder[A, Ko, Ko, Ko] = Builder[A, Ko, Ko, Ko]()

  /** Starts the creation of a StateTypeSignature for an unkeyed co-process function, with an empty builder. */
  def forUnkeyedCoProcessFunction[A <: CoProcessFunction[_, _, _] : ClassTag]: Builder[A, Ok, Ko, Ok] = Builder[A, Ok, Ko, Ok]()

  /** Starts the creation of a StateTypeSignature for a keyed co-process function, with an empty builder. */
  def forKeyedCoProcessFunction[A <: KeyedCoProcessFunction[_, _, _, _] : ClassTag]: Builder[A, Ko, Ko, Ok] = Builder[A, Ko, Ko, Ok]()


  /** A builder for the StateTypeSignature of a function. This Builder uses phantom types to make sure signatures are
   * set for everything needed: key, state, and broadcast state. */
  case class Builder[A, KS <: Status, SS <: Status, BSS <: Status] private[StateTypeSignature](
      private val content: StateTypeSignature[A] = new StateTypeSignature[A](Nil)
  )(implicit tag: ClassTag[A]) {

    def withKey[B](implicit signature: TypeSignature[B]): Builder[A, Ok, SS, BSS] =
      copy(content = content.appendSignature(signature))

    def noState: Builder[A, KS, Ok, BSS] = copy()

    def noBroadcastState: Builder[A, KS, SS, Ok] = copy()

    /** Passes the type for the value state */
    def withValueState[B](implicit bSign: TypeSignature[B]): Builder[A, KS, Ok, BSS] =
      copy(content = content.appendString("ValueState").appendSignature(bSign))

    /** Passes the type for the list state */
    def withListState[B](implicit bSign: TypeSignature[B]): Builder[A, KS, Ok, BSS] =
      copy(content = content.appendString("ListState").appendSignature(bSign))

    /** Passes the type for the map state */
    def withMapState[K, V](implicit kSign: TypeSignature[K], vSign: TypeSignature[V]): Builder[A, KS, Ok, BSS] =
      copy(content = content.appendString("MapState").appendSignature(kSign).appendSignature(vSign))

    /** Commodity method: passes the descriptor instead of explicitly passing the type. */
    def withStateDescriptor[B: TypeSignature](desc: ValueStateDescriptor[B]): Builder[A, KS, Ok, BSS] =
      withValueState[B]

    /** Commodity method: passes the descriptor instead of explicitly passing the type. */
    def withStateDescriptor[B: TypeSignature](desc: ListStateDescriptor[B]): Builder[A, KS, Ok, BSS] =
      withListState[B]

    /** Commodity method: passes the descriptor instead of explicitly passing the types. */
    def withStateDescriptor[K: TypeSignature, V: TypeSignature](desc: MapStateDescriptor[K, V]): Builder[A, KS, Ok, BSS] =
      withMapState[K, V]

    /** Passes the key and value for the broadcast state */
    def withBroadcastState[K, V](implicit kSign: TypeSignature[K], vSign: TypeSignature[V]): Builder[A, KS, SS, Ok] =
      copy(content = content.appendString("BroadcastState").appendSignature(kSign).appendSignature(vSign))

    /** Commodity method: passes the descriptor instead of explicitly passing the types. */
    def withBroadcastStateDescriptor[K: TypeSignature, V: TypeSignature](desc: MapStateDescriptor[K, V]): Builder[A, KS, SS, Ok] =
      withBroadcastState[K, V]

    /** Generic function to use when the more specific ones don't work. */
    def withType[B](implicit typeInfo: TypeSignature[B]): Builder[A, KS, SS, BSS] =
      copy(content = content.appendSignature(typeInfo))

    def build(implicit evK: KS =:= Ok, evS: SS =:= Ok, evBss: BSS =:= Ok): StateTypeSignature[A] =
      content.appendString(tag.runtimeClass.getName)
  }

  object Builder {

    sealed trait Status

    trait Ok extends Status

    trait Ko extends Status

  }

}
