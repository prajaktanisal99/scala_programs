import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.sql._

object Trip {
  case class TripData ( distance: Int, amount: Float )

  def main ( args: Array[ String ] ): Unit = {
    val conf = new SparkConf().setAppName("Trip")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val trip_data: RDD[TripData]
      = sc.textFile(args(0)).flatMap(line => {
      val values = line.split(",")
      val distanceStr = values(4)
      val isFirstCharAlphabet = distanceStr.headOption.exists(_.isLetter)
      if (isFirstCharAlphabet) {
        None
      } else {
        try {
          val distance = distanceStr.toFloat.round.toInt
          val amount = values(values.length - 1).toFloat
          Some(TripData(distance, amount))
        } catch {
          case _: NumberFormatException =>
            None
        }
      }
    })

    val trip_df = trip_data.toDF.createOrReplaceTempView("trip_data")
    val query = spark.sql("SELECT ROUND(distance) AS distance, AVG(amount) AS avg FROM trip_data WHERE distance < 200 GROUP BY distance ORDER BY distance")
    
    println("queryyyyy")
    query.show()

    val res = query.collect()
    
    res.foreach { case Row( distance: Int, avg: Double )
                              => println("%3d\t%3.3f".format(distance,avg)) }

  }
}
