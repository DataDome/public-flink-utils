package co.datadome.flinkutils.syntax

import co.datadome.flinkutils.util.StateTypeSignature
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.DataStream

/** Typeclass for Flink elements than can receive a name and an UID. */
trait FlinkCanName[A] {

  def name(a: A)(n: String): A

  def uid(a: A)(i: String): A

  /** Sets both UID and name to the same value. */
  @inline final def uidName[S](a: A)(n: String): A = uid(name(a)(n))(n)

  /** Sets the name an a state-based UID. The name is fixed to the value in argument, but the UID includes a hash of the
   * state signature for the type parameter. Therefore, any ulterior modification to the structure of the state will
   * make the UID change as well. This helps ensure that on any update of the Flink job, the state is recovered only if
   * the state structure has not changed.
   * @tparam S State for which there must exists a StateSignature, typically a ProcessFunction.
   */
  @inline final def uidNameStated[S](a: A)(n: String)(implicit signature: StateTypeSignature[S]): A = uid(name(a)(n))(s"$n-[${signature.hash}]")
}

object FlinkCanName {
  def apply[A: FlinkCanName]: FlinkCanName[A] = implicitly[FlinkCanName[A]]

  implicit def canNameStream[A]: FlinkCanName[DataStream[A]] = new FlinkCanName[DataStream[A]] {
    override def name(as: DataStream[A])(n: String): DataStream[A] = as.name(n)

    override def uid(as: DataStream[A])(i: String): DataStream[A] = as.uid(i)
  }

  implicit def canNameFlink[A]: FlinkCanName[DataStreamSink[A]] = new FlinkCanName[DataStreamSink[A]] {
    override def name(as: DataStreamSink[A])(n: String): DataStreamSink[A] = as.name(n)

    override def uid(a: DataStreamSink[A])(i: String): DataStreamSink[A] = a.uid(i)
  }

  trait FlinkCanNameOps[A] {
    def typeClassInstance: FlinkCanName[A]

    def self: A

    /** Sets both UID and name to the same value. Use it on stateless components. */
    @inline final def uidName(name: String): A =
      typeClassInstance.uidName(self)(name)

    /** Sets a name and an UID, including into the UID the state signature */
    @inline final def uidNameStated[S](name: String)(implicit signature: StateTypeSignature[S]): A =
      typeClassInstance.uidNameStated[S](self)(name)
  }

  trait syntax {
    implicit def toFlinkCanNameOps[A: FlinkCanName](target: A): FlinkCanNameOps[A] = new FlinkCanNameOps[A] {
      @inline val self: A = target
      @inline val typeClassInstance: FlinkCanName[A] = FlinkCanName[A]
    }
  }

  object syntax extends syntax

}
