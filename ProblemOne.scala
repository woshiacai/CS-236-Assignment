import org.apache.spark.sql.Encoders
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.simba.SimbaSession
import org.apache.spark.sql.simba.index.RTreeType
import org.apache.spark.sql.types.StructType

object ProblemOne {
  case class TrajData(tid: Long, Oid: Int, x: Double, y: Double, date: String)
  def main(args: Array[String]): Unit = {

    val simba = SimbaSession
      .builder()
      .master("local[4]")
      .appName("Project")
      .config("simba.index.partitions", "8")
      .getOrCreate()

    import simba.implicits._

    val schema = ScalaReflection.schemaFor[TrajData].dataType.asInstanceOf[StructType]

    var points = simba.read
      .option("header", "false")
      .schema(schema)
      .csv("/Users/harry/Desktop/DBMS/trajectories.csv")
      .as[TrajData]

    import simba.simbaImplicits._

    def getMinMaxPoint(pd: Iterator[TrajData]): String = {
      var min = pd.next()
      var maxx = min.x
      var minx = min.x
      var maxy = min.y
      var miny = min.y

      pd.foreach(pnt => {
        if(pnt.x < minx) {
          minx = pnt.x
        }
        else if(pnt.x > maxx) {
          maxx = pnt.x
        }
        else if(pnt.y < miny) {
          miny = pnt.y
        }
        else if(pnt.y > maxy) {
          maxy = pnt.y
        }
      })
      val poly = "Polygon(("+minx+" "+miny+","+minx+" "+maxy+","+maxx+" "+maxy+","+maxx+" "+miny+","+minx+" "+miny+"))"
      poly
    }

    points = points.sample(false,0.1)
    points.index(RTreeType,"index",Array("x","y"))

    val mbrs = points.mapPartitions(partition =>
      Iterator(getMinMaxPoint(partition))
    )(Encoders.STRING)

    mbrs.coalesce(1).write.option("header", "true").csv("MBR")

  }
}
