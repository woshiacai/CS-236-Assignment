import java.time.LocalDate
import java.time.format.DateTimeFormatter

import ProblemTwo.{PointData, TrajectoryData, problemFour}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.types.StructType
import org.joda.time.format.DateTimeFormat

object ProblemThree {

  def main(args: Array[String]): Unit = {
    val simba = SimbaSession
      .builder()
      .master("local[3]")
      .appName("Project")
      .config("spark.driver.memory","12g")
      .config("spark.executor.memory","12g")
      .config("simba.index.partitions", "8")
      .getOrCreate()
    problemThreeTwo(simba)
    simba.stop()
  }

  private def problemThreeOne(simba:SimbaSession): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    val time = System.nanoTime

    val trajectoryschema = ScalaReflection.schemaFor[TrajectoryData].dataType.asInstanceOf[StructType]
    var trajectories = simba.read
      .option("header","false")
      .schema(trajectoryschema)
      .csv("/Users/harry/Desktop/DBMS/trajectories.csv")
      .as [TrajectoryData]

    trajectories = trajectories.sample(false,0.1)
    //MARK:- Problem 3-1

      import org.apache.spark.sql.functions._
      var temp = trajectories.filter(data => {
        val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        (LocalDate.parse(data.date, dateFormat).getMonthValue >= 3 && LocalDate.parse(data.date, dateFormat).getMonthValue <= 6) && LocalDate.parse(data.date, dateFormat).getYear == 2008
      }).toDF().drop("oid", "tid", "date").distinct()

      val t1 = temp.toDF("x1", "y1")

      temp.distanceJoin(t1, Array("x", "y"), Array("x1", "y1"), 500).distinct().groupBy("x", "y").count().sort(desc("count")).count()

      val duration = (System.nanoTime - time) / 1e9d

      print("Time:"+ duration)

  }
  private def problemThreeTwo(simba:SimbaSession): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    val pointschema = ScalaReflection.schemaFor[PointData].dataType.asInstanceOf[StructType]
    var points = simba.read
      .option("header", "false")
      .schema(pointschema)
      .csv("/Users/harry/Desktop/DBMS/POIs.csv")
      .as[PointData].toDF().drop("tag","pid")

    val trajectoryschema = ScalaReflection.schemaFor[TrajectoryData].dataType.asInstanceOf[StructType]
    var trajectories = simba.read
      .option("header","false")
      .schema(trajectoryschema)
      .csv("/Users/harry/Desktop/DBMS/trajectories.csv")
      .as [TrajectoryData]

    val toDay = udf((dat:String) => {
      val date = dat.split(" ")(0)
      val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
      val dt = fmt.parseDateTime(date)
      dt.toString("EEEEE")
    })
    val toMonth = udf((dat:String) => {
      val date = dat.split(" ")(0)
      val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
      val dt = fmt.parseDateTime(date)
      dt.toString("MMMM")
    })

    val traj = trajectories
      .withColumn("day",toDay($"date")).toDF()

    val f = List("Sunday","Saturday")

    val weekdays = traj.filter(!traj("day").isin(f: _*))
      .drop("day").toDF("tid","oid","x1","y1","date")

    val finalDF = weekdays.filter(row => {
      val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      LocalDate.parse(row.getAs[String]("date"),dateFormat).getYear == 2008 || LocalDate.parse(row.getAs[String]("date"),dateFormat).getYear == 2009
    }).drop("date","pid").cache()

    //MARK:- Problem 3-2
    val time = System.nanoTime
    import org.apache.spark.sql.functions._

    val inter = points.distanceJoin(finalDF,leftKeys = Array("x","y"), rightKeys = Array("x1","y1"),r = 500).drop("x1","y1")
    val df = inter.groupBy("x","y").agg(countDistinct("tid","oid").alias("count")).sort(desc("count")).drop("tid","oid")
    df.count()
    val duration = (System.nanoTime - time) / 1e9d

    print("Time:"+ duration)

  }
}
