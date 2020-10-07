import sbt._

object Dependencies {

  object Scala {
    /* Flink 1.8.0 is not compatible with an ulterior version, as Scala 2.12.8 broke binary compatibility.
    See: https://issues.apache.org/jira/browse/FLINK-12461 */
    val Version = "2.12.7"
  }

  object Shapeless {
    val Shapeless = "com.chuusai" %% "shapeless" % "2.3.3"

    val All: Seq[ModuleID] = Seq(Shapeless)
  }

  object Flink {
    val Version = "1.10.1"

    val Core = "org.apache.flink" %% "flink-scala" % Version % Provided
    val Streaming = "org.apache.flink" %% "flink-streaming-scala" % Version % Provided

    val All: Seq[ModuleID] = Seq(Core, Streaming)
  }


  object Testing {
    val ScalaTest = "org.scalatest" %% "scalatest" % "3.2.0" % Test

    val All: Seq[ModuleID] = Seq(ScalaTest)
  }

}
