
val sqlcontext = new org.apache.spark.sql.SQLContext(sc)


jioData.take(2).map(_.split("[\\p{Punct}\\s]+"))

val s ="string"
s.split("[\\p{Punct}\\s]+")




val jioData = sc.textFile("jio.txt")
val words = jioData.flatMap(_.split("[\\p{Punct}\\s]+"))
val tag = words.zipWithIndex

val s =tag.map(x=> x._1+","+x._2)

myrdd.map(a => a._1 + "," + a._2.mkString(",")).saveAsTextFile
s.saveAsTextFile("wordsAndNumber.csv")


















df.write.format("com.databricks.spark.csv").option("header", "true").save("file.csv")
It also support reading from csv file with similar API

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("file.csv")


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


