lazy val commonSettings = Seq(
    organization := "org.robertdodier",
    normalizedName := "spark-wilcoxon",
    version := "0.0.1-SNAPSHOT",
    name := "spark-wilcoxon",
    organizationName := "Personal organization or lack thereof of Robert Dodier",
    description := "Compute Wilcoxon-Mann-Whitney rank sum statistic in Apache Spark.",
    organizationHomepage := Some(url("https://github.com/robert-dodier")),
    scalaVersion := "2.10.6"
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    parallelExecution in Test := false,
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.5.1" % "provided",
      "org.apache.spark" %% "spark-mllib" % "1.5.1" % "provided",
      "org.scalatest" %% "scalatest" % "2.2.1" % "test"
    ),
    resolvers += "Akka Repository" at "http://repo.akka.io/releases/",
    publishMavenStyle := true,
    publishTo := {
      val nexus = "https://oss.sonatype.org/"
      if (isSnapshot.value)
        Some("snapshots" at nexus + "content/repositories/snapshots")
      else
        Some("releases"  at nexus + "service/local/staging/deploy/maven2")
    },
    publishArtifact in Test := false,
    pomIncludeRepository := { _ => false },
    test in assembly := {},
    assemblyJarName in assembly := normalizedName.value + "_" + scalaVersion.value + "-" + version.value + "-ASSEMBLY.jar",
    licenses := Seq("Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0.html")),
    homepage := Some(url("https://github.com/robert-dodier/spark-wilcoxon")),
    pomExtra := (
        <scm>
          <url>git@github.com:robert-dodier/spark-wilcoxon.git</url>
          <connection>scm:git:git@github.com:robert-dodier/spark-wilcoxon.git</connection>
        </scm>
        <developers>
          <developer>
            <id>robert-dodier</id>
            <name>Robert Dodier</name>
            <email>robert.dodier@gmail.com</email>
          </developer>
        </developers>)
  )

