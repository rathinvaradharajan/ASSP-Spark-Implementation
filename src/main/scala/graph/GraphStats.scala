package graph

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object GraphStats {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger

    //Check for invalid arguments
    if (args.length != 2) {
      logger.error("Usage:\n<input dir> <output dir>")
      System.exit(1)
    }

    //Set the configuration
    val conf = new SparkConf().setAppName("Graph Stats").setMaster("local")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development
    val hadoopConf = new org.apache.hadoop.conf.Configuration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    try {
      hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true)
    } catch {
      case _: Throwable => {}
    }

    //Total number of vertices
    val numOfVertices = 10

    // Compute the distance Matrix RDD which contains distances between any two vertices.
    var distMatrix_1 = sc.textFile(args(0)).map(line => {
      val num = line.substring(1, line.length - 1).split(",") //Split each line using comma
      (num(0).trim.toLong, num(1).trim.toLong, num(2).trim.toLong) //Write the following values: from, to and weight to the RDD
    }).filter(u => u._1 < numOfVertices && u._2 < numOfVertices) //Filter the vertices according to the max limit.

    // Copy the distMatrix by having the row as key.
    var distMatrix_2 = distMatrix_1.map(n => (n._1, n))

    // Computing square of adjacencyMatrix gives us the all shortest paths of length 2.
    // Square of the above matrix gives us the all shortest paths of length 4.
    // Any shortest path doesn't contain more than V-1 edges where V is the number of vertices.
    // Hence repeating the above for sqrt(V) iterations gives us all possible shortest paths.

    for (itr <- 0 to Math.sqrt(numOfVertices).toInt) {
      val multiply = distMatrix_1.map(m => (m._2, m)) //Distance matrix by having column as key
        .join(distMatrix_2) //Join the matrix by using Column-By-Row partitioning.
        .map({ case (k, (matrix_m, matrix_n)) =>
          ((matrix_m._1, matrix_n._2), matrix_m._3 + matrix_n._3) //Add the corresponding cells of the matrix.
        }).reduceByKey((dist1, dist2) => Math.min(dist1, dist2)) //

      distMatrix_1 = distMatrix_1
        .map(m => ((m._1, m._2), m._3)) //Format the adj matrix
        .union(multiply) // Do a union of prev and multiplied matrix
        .reduceByKey((dist1, dist2) => Math.min(dist1, dist2)) // Update the value of the cell ony if it's lesser than before.
        .map(value => (value._1._1, value._1._2, value._2)) // Format the output

      distMatrix_2 = distMatrix_1.map(value => (value._1, value)) //Update for next iteration
    }

    // Calculate the diameter of the graph.
    val diameter = sc.parallelize(Array(distMatrix_1.max()(new Ordering[(Long, Long, Long)]() {
      override def compare(x: (Long, Long, Long), y: (Long, Long, Long)): Int = Ordering[Long].compare(x._3, y._3)
    })._3))

    //Write the shortest path output.
    distMatrix_1.saveAsTextFile(args(1))

    //Write the the diameter of the graph.
    diameter.saveAsTextFile(args(1))
  }
}