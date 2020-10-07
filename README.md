This is a **PUBLIC** repository for some tools we use in our Flink jobs.

# State signature
This makes the UID of an operator change automatically whenever there is some change in the state structure, making sure Flink will not try to reload the old state.

In your model classes:
```scala
  import co.datadome.flinkutils.util.TypeSignature

  case class BotBuster(name: String)

  object BotBuster {
    implicit val typeSignature: TypeSignature[BotBuster] =
      TypeSignature.forSimpleCaseClass[BotBuster]
  }

  class Something { … }

  object Something {
    implicit val typeSignature: TypeSignature[Something] =
      TypeSignature[Something]
        .withType[String]
        .withType[BotBuster]
        .build
  }
```
In your operator:
```scala
  import co.datadome.flinkutils.util.{StateTypeSignature, TypeSignature}
  import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction

  class MyKeyedBroadcastProcessFunction extends KeyedBroadcastProcessFunction[Int, String, Boolean, Int] { … }

  object MyKeyedBroadcastProcessFunction {
    val stateSignature = StateTypeSignature.forKeyedBroadcastFunction[MyKeyedBroadcastProcessFunction]
      .withKey[Int]
      .withValueState[Set[String]]
      .withBroadcastState[Int, BotBuster]
      .build
  }
```
In your job:
```scala
  import org.apache.flink.streaming.api.scala._
  import co.datadome.flinkutils.syntax._

  val stream: DataStream[String] = ???
  stream.uidNameStated[MyKeyedBroadcastProcessFunction]("my-function")
```
