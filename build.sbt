organization  := "net.sognefest"

name := "BitstampCollector"

version := "1.0"

scalaVersion  := "2.11.1"

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

libraryDependencies ++= {
  Seq(
    "com.typesafe.slick"      %%  "slick"                       % "2.1.0-M2",
    "org.slf4j"               %   "slf4j-nop"                   % "1.6.4",
    "org.postgresql"          %   "postgresql"                  % "9.3-1101-jdbc41",
    "org.json4s"              %%  "json4s-native"               % "3.2.9",
    "com.pusher"              %   "pusher-java-client"          % "0.3.1"
  )
}


