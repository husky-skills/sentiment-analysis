

import org.apache.log4j.{Level, Logger} 

val rootLogger = Logger.getRootLogger()

val jioData = sc.textFile("jio.txt")
val words = jioData.flatMap(_.split("[\\p{Punct}\\s]+"))
val dwords = words.distinct
val mp = dwords.zipWithIndex.collect.toMap

val lines = jioData.map(_.split("[\\p{Punct}\\s]+")).collect.toArray
lines.map(l => l.map(w => mp(w)))

val changed = lines.map(l => l.map(w => mp(w)))
changed.foreach(a => rootLogger.warn(a.foldLeft(""){(x,y) => x+" "+y}))