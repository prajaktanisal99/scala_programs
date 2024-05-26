import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark._
import org.apache.spark.rdd._

object Graph {
  def main(args: Array[String]) {
      val conf = new SparkConf().setAppName("Graph")
      val sc = new SparkContext(conf)

      // A graph is a dataset of vertices, where each vertex is a triple
      // (group,id,adj) where id is the vertex id, group is the group id
      // (initially equal to id), and adj is the list of outgoing neighbors
      var graph: RDD[ ( Long, Long, List[Long] ) ]
         = sc.textFile(args(0)).map(line => {val values = line.split(",").map(_.toLong)     
        (values(0),values(0),values.tail.toList) })
     

       for ( i <- 1 to 5 ) {
         // For each vertex (group,id,adj) generate the candidate (id,group)
         //    and for each x in adj generate the candidate (x,group).
         // Then for each vertex, its new group number is the minimum candidate
         val groups: RDD[(Long,Long)]
            = graph.flatMap(value => value._3.flatMap(adj => Seq((adj,value._1))) ++ Seq((value._2, value._1)))
            .reduceByKey((x,y) => if(x<y) x else y)
       
         // reconstruct the graph using the new group numbers
         graph = groups.join(graph.map(value => (value._2,value)))
            .map{case(id,(minimum,value)) => (minimum,id,value._3)}
      }

      // Print the group sizes
      val groupCount = graph.map { case (group, _, _) => (group, 1) }
      .reduceByKey((x, y) => x + y)
      groupCount.collect()

      for (count <- groupCount) println(s"${count._1} ${count._2}")

      sc.stop()
  }
}

