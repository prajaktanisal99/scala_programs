import org.apache.spark._
import org.apache.spark.rdd._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Add {
  case class Block ( rows: Int, columns: Int, data: Array[Double] ) extends Serializable {
    override
    def toString (): String = {
      var s = "\n"
      for ( i <- 0 until rows ) {
        for ( j <- 0 until columns )
          s += " %.2f".format(data(i*columns+j))
        s += "\n"
      }
      s
    }
  }

  
  /* Convert a list of triples (i,j,v) into a Block */
  def toBlock ( rows: Int, columns: Int, triples: List[(Int,Int,Double)] ): Block = {
    
    val value_list = Array.fill(rows * columns)(0.0) 
    for ((i, j, v) <- triples) {
      // if (i == 8 && j == 12)
      //   {println(s"$i:: $j -> $v")}
      // println(s"tttt:: ${((i % rows) * columns) + (j % columns)}")
      value_list((i % rows) * columns + (j % columns)) = v 
    }
    Block(rows, columns, value_list)
  }

  
  /* Add two Blocks */
  def blockAdd ( m: Block, n: Block ): Block = {
    val result_blocks = m.data.zip(n.data).map { case (x, y) => x + y }
    Block(m.rows, m.columns, result_blocks)
  }
  
  /* Read a sparse matrix from a file and convert it to a block matrix */
  def createBlockMatrix ( sc: SparkContext, file: String, rows: Int, columns: Int ): RDD[((Int,Int),Block)]
   = {

    val lines = sc.textFile(file)
    val data = lines.map { line =>
      val Array(i, j, value) = line.split(",").map(_.trim)
      ((i.toInt, j.toInt), value.toDouble)
    }
    
    // group by i/rows and j/cols
    val group_by_block = data.groupBy { case ((i, j), _) => (i / rows, j / columns) }
    // group_by_block.collect().foreach {
    //   case ( key, values) => {
    //     val new_row = key._1
    //     val new_col = key._2
    //     println(s"grouped => $new_row:: $new_col (${values.mkString(", ")})")
    //   }
    // }
    
    val blocks = group_by_block.map { case ((r, c), values) =>
      val triples = values.map { case ((i, j), v) => (i, j, v.toDouble) }.toList
      val block = toBlock(rows, columns, triples)
      ((r, c), block)
    }
    blocks
  }


  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Add block matrix")
    conf.setMaster("local[2]")

    val sparkContext = new SparkContext(conf)
    
    // 1. get rows and columns
    val rows = args(0)
    val columns = args(1)


    // 2. convert sparse to block
    // val block_m = sparkContext.textFile(args(3)).map(line => line)
    val blocks_for_m = createBlockMatrix(sparkContext, args(2), rows.toInt, columns.toInt)
    val blocks_for_n = createBlockMatrix(sparkContext, args(3), rows.toInt, columns.toInt)


    val result_blocks = blocks_for_m.join(blocks_for_n).mapValues { case (blocks_m, blocks_n) =>
      blockAdd(blocks_m, blocks_n)
    }

    val result_to_print = result_blocks.filter {
      case (key, value) => key == (1,2)
    }

    // res.foreach {
    //   case (key,value) => println
    // }

    result_to_print.map(_.toString).saveAsTextFile("output")   
    sparkContext.stop()
  }
}
