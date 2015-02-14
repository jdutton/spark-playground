// Make fat jar with all dependencies
addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.12.0")

// Support building Eclipse project files by running `sbt eclipse`
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.5.0")

// Support building IntelliJ IDEA project files by running `sbt gen-idea`
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")

// Format source files
addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.3.0")

