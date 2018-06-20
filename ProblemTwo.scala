import java.time.{LocalDate, LocalTime}
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.spatial.{MBR, Point}
import org.apache.spark.sql.types.StructType
import org.joda.time.format.DateTimeFormat

object ProblemTwo {
  case class PointData(pid: Long, tag: String, x: Double, y: Double)
  case class TrajectoryData(tid: Long, oid: Long, x: Double, y: Double, date: String)
  case class TrajectoryDataModified(tid: Long, date: String, oid: Long, x: Double, y: Double)

  def main(args: Array[String]): Unit = {
    val simba = SimbaSession
      .builder()
      .master("local[*]")
      .appName("Project")
        .config("spark.driver.memory","8g")
        .config("spark.executor.memory","8g")
      .config("simba.index.partitions", "8")
      .getOrCreate()

    //problemOne(simba)
    //problemTwo(simba)
    problemThree(simba)
    //problemFour(simba)
    //problemFive(simba)
    simba.stop()
  }

  private def problemOne(simba: SimbaSession): Unit = {

    import simba.implicits._
    import simba.simbaImplicits._
    val pointschema = ScalaReflection.schemaFor[PointData].dataType.asInstanceOf[StructType]
    var points = simba.read
      .option("header", "false")
      .schema(pointschema)
      .csv("/Users/harry/Desktop/DBMS/POIs.csv")
      .as[PointData]

    //Mark:- Problem 2-1
    val res = points.range(Array("x","y"), point1 = Array(-339220.0,4444725.0), point2 = Array(-309375.0,4478070.0))
        .filter(row => row.getAs[String]("tag").contains("restaurant"))
    res.coalesce(1).write.option("header","true").csv("Q1")
  }

  private def problemTwo(simba: SimbaSession): Unit = {

    import simba.implicits._
    import simba.simbaImplicits._

    val trajectoryschema = ScalaReflection.schemaFor[TrajectoryData].dataType.asInstanceOf[StructType]
    var trajectories = simba.read
      .option("header","false")
      .schema(trajectoryschema)
      .csv("/Users/harry/Desktop/DBMS/trajectories.csv")
      .as [TrajectoryData]

    //MARK:- Problem 2-2

    import org.apache.spark.sql.functions._
    val toHour = udf((date: String) => {
      val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      LocalTime.parse(date,dateFormat).getHour
    })
    val toDay = udf((dat:String) => {
            val date = dat.split(" ")(0)
            val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
            val dt = fmt.parseDateTime(date)
            dt.toString("EEEEE")
    })

    val traj = trajectories.circleRange(Array("x","y"), point = Array(-322357.0,4463408.0),2000)
      .withColumn("day",toDay($"date")).toDF()

    val f = List("Sunday","Saturday")
    val res = traj.filter(!traj("day").isin(f: _*))
      .toDF().withColumn("hour",toHour($"date"))
      .groupBy("hour")
      .agg(countDistinct("tid","oid")
      .alias("perhour"))
      .agg(avg("perhour"))
      .first()
      .get(0)

    print(res.asInstanceOf[Double])
  }

  private def problemThree(simba:SimbaSession): Unit = {

    val midpoint = Point(Array((-339220-309375)/2.0,(4444725+4478070)/2.0))
    val ltmbr = MBR(low = Point(Array(-339220.0,(4444725+4478070)/2.0)), high = Point(Array((-339220-309375)/2.0,4478070.0)))
    val rtmbr = MBR(low = midpoint, high = Point(Array(-309375.0,4478070.0)))
    val lbmbr = MBR(low = Point(Array(-339220.0,4444725.0)), high = midpoint)
    val rbmbr = MBR(low = Point(Array((-339220-309375)/2.0,4444725.0)), high = Point(Array(-309375.0,(4444725+4478070)/2.0)))

    import simba.implicits._
    import simba.simbaImplicits._
    import org.apache.spark.sql.functions._


    val trajectoryschema = ScalaReflection.schemaFor[TrajectoryDataModified].dataType.asInstanceOf[StructType]
    var trajectories = simba.read
      .option("header","false")
      .schema(trajectoryschema)
      .csv("/Users/harry/IdeaProjects/DBMS/StartEndPoints/merged.csv")
      .as [TrajectoryData]

    //MARK:- Problem 2-3

    val startEndPoints = trajectories.groupBy($"tid").agg(min("date").alias("date")).sort("tid")
      .union(trajectories.groupBy($"tid").agg(max("date").alias("date")).sort("tid"))
    val positions = trajectories.join(startEndPoints,usingColumns = Seq("tid","date")).sort("tid").drop("oid")
    //positions.write.option("header",false).csv("StartEndPoints")

    var pointsInRegion = positions.range(Array("x","y"),point1 = Array(-339220.0,4444725.0), point2 = Array(-309375.0,4478070.0)).groupBy("tid").agg(count("tid").alias("count")).where($"count" === 2)
    pointsInRegion = pointsInRegion.join(positions,usingColumn = "tid").drop("count")
    var sum = 0.asInstanceOf[Long]
    var sum1 = 0.asInstanceOf[Long]
    for(mbr<-Seq(ltmbr,rtmbr,lbmbr,rbmbr)) {
      sum = sum + pointsInRegion.range(Array("x","y"),point1 = Array(mbr.low.coord(0),mbr.low.coord(1)), Array(mbr.high.coord(0),mbr.high.coord(1)))
        .groupBy("tid").agg(count("tid").alias("cnt")).select("tid").where($"cnt"===2).count()

    }
    for(mbr<-Seq(ltmbr,rtmbr,lbmbr,rbmbr)) {
      sum1 = sum1 + pointsInRegion.range(Array("x","y"),point1 = Array(mbr.low.coord(0),mbr.low.coord(1)), Array(mbr.high.coord(0),mbr.high.coord(1)))
        .groupBy("tid").agg(count("tid").alias("cnt")).select("tid").where($"cnt" === 1).count()

    }
    print(sum) //144577
    print(sum1)  //14378
  }

  private def problemFour(simba:SimbaSession): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._

    val trajectoryschema = ScalaReflection.schemaFor[TrajectoryData].dataType.asInstanceOf[StructType]
    var trajectories = simba.read
      .option("header","false")
      .schema(trajectoryschema)
      .csv("/Users/harry/Desktop/DBMS/trajectories.csv")
      .as [TrajectoryData]

    trajectories = trajectories.sample(false,0.1)
    //MARK:- Problem 2-4
    import org.apache.spark.sql.functions._
    var temp = trajectories.filter(data => {
      val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      (LocalDate.parse(data.date,dateFormat).getMonthValue >= 3 && LocalDate.parse(data.date,dateFormat).getMonthValue <=6) && LocalDate.parse(data.date,dateFormat).getYear == 2008
    }).toDF().drop("oid","tid","date").distinct()

    val t1 = temp.toDF("x1","y1")

    temp.distanceJoin(t1,Array("x","y"),Array("x1","y1"),100).distinct().groupBy("x","y").count().sort(desc("count"))
      .write.option("header","true").csv("Q4")
  }

  private def problemFive(simba:SimbaSession): Unit = {
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
      .drop("day").toDF("tid","oid","x1","y1","date").cache()

    val traj2008 = weekdays.filter(row => {
      val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      LocalDate.parse(row.getAs[String]("date"),dateFormat).getYear == 2008
    })

    val traj2009 = weekdays.filter(row => {
      val dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
      LocalDate.parse(row.getAs[String]("date"),dateFormat).getYear == 2009
    })

    //MARK:- Problem 2-5


    import org.apache.spark.sql.functions._
    val inter = points.distanceJoin(traj2009,leftKeys = Array("x","y"), rightKeys = Array("x1","y1"),r = 100).drop("x1","y1","pid")
      .withColumn("month",toMonth($"date")).drop("date")
    val df = inter.groupBy("month","x","y").agg(countDistinct("tid","oid").alias("count")).sort(desc("count")).drop("tid","oid")

    import org.apache.spark.sql.expressions.Window
    import org.apache.spark.sql.functions.{rank, desc}

    val n: Int = 10
    // Window definition
    val w = Window.partitionBy($"month").orderBy(desc("count"))

    // Filter
    df.withColumn("rank", rank.over(w)).where($"rank" <= n).coalesce(1).write.csv("result1")

  }

}
