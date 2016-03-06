name := "physician_compare"

version := "1.0"

scalaVersion := "2.11.7"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

libraryDependencies += "com.typesafe.play" %% "play" % "2.3.9"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.6.0"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0"

// The Typesafe repository
resolvers += "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"

//Merge Strategy
assemblyMergeStrategy in assembly := {

  case x if x.startsWith("org/apache/hadoop/yarn/") => MergeStrategy.first
  case x if x.startsWith("org/apache/commons/beanutils/") => MergeStrategy.first
  case x if x.startsWith("org/apache/spark/unused/") => MergeStrategy.first
  case x if x.startsWith("org/slf4j/impl/") => MergeStrategy.first
  case x if x.startsWith("org/apache/commons/collections/") => MergeStrategy.first
  case x if x.startsWith("play/core/server/ServerWithStop") => MergeStrategy.first
  case x if x.startsWith("com/google/common/base/") => MergeStrategy.first
  case x if x.startsWith("com/esotericsoftware/minlog/") => MergeStrategy.first
  case x if x.startsWith("javax/xml/") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}