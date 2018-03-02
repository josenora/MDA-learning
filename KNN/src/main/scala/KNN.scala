/* WordCount.scala */
import org.apache.spark._
import org.apache.hadoop.fs._

object KNN {
    def main(args: Array[String]) {
        val files = args(0)
        // val test_file = args(1)
        // val outputPath = args(1)

        val conf = new SparkConf().setAppName("KNN")
        val sc = new SparkContext(conf)

        // // Cleanup output dir
        // val hadoopConf = sc.hadoopConfiguration
        // val hdfs = FileSystem.get(hadoopConf)
        // try { hdfs.delete(new Path(outputPath), true) } catch { case _ : Throwable => { } }

        // println("\n Main function !!!") // show on stdout

        val lines = sc.textFile(files, 32)
        // val testlines = sc.textFile(test_file, 32)

        val trainSet = lines.map(line => {  
            var datas = line.split(",")  

            var p0 = datas(0).toInt  // id
            var p1 = datas(1).toDouble
            var p2 = datas(2).toDouble  
            var p3 = datas(3).toDouble  
            var p4 = datas(4).toDouble  
            var p5 = datas(5).toDouble
            var p6 = datas(6).toDouble  
            var p7 = datas(7).toDouble  
            var p8 = datas(8).toDouble  
            var p9 = datas(9).toDouble
            var p10 = datas(10).toDouble  
            var p11 = datas(11).toDouble  
            var p12 = datas(12).toDouble  
            var p13 = datas(13).toDouble
            var p14 = datas(14).toDouble  
            var p15 = datas(15).toDouble  
            var p16 = datas(16).toDouble  
            var p17 = datas(17).toDouble
            var p18 = datas(18).toDouble  
            var p19 = datas(19).toDouble   // label

            (p0, (p1, p2, p3, p4, p5, p6, p7, p8, p9, p10, p11, p12, p13, p14, p15, p16, p17, p18, p19))  
        })

        // val testSet = testlines.map(line => line.toInt)

        val kValue = args(1).toInt
        val query_id = args(2).toInt
        val query = trainSet.lookup(query_id)

        val bquery = sc.broadcast(query)

        val result_array = trainSet.map(line => {  
            var q = bquery.value(0) 
            var distance = Math.sqrt(Math.pow(q._1 - line._2._1, 2) + Math.pow(q._2 - line._2._2, 2) + Math.pow(q._3 - line._2._3, 2) + Math.pow(q._4 - line._2._4, 2) + Math.pow(q._5 - line._2._5, 2) + Math.pow(q._6 - line._2._6, 2) + Math.pow(q._7 - line._2._7, 2) + Math.pow(q._8 - line._2._8, 2) + Math.pow(q._9 - line._2._9, 2) + Math.pow(q._10 - line._2._10, 2) + Math.pow(q._11 - line._2._11, 2) + Math.pow(q._12 - line._2._12, 2) + Math.pow(q._13 - line._2._13, 2) + Math.pow(q._14 - line._2._14, 2) + Math.pow(q._15 - line._2._15, 2) + Math.pow(q._16 - line._2._16, 2) + Math.pow(q._17 - line._2._17, 2) + Math.pow(q._18 - line._2._18, 2))  

            (line._1, distance, line._2._19)  
        }).sortBy(_._2).take(kValue + 1)

        val result_array_filterd = result_array.filterNot(_._1 == query_id)

        // println("\n\n\n")
        result_array_filterd.map(x =>{
            println(x)
        })
        println("\n -----------------------------------------")

        val class_a_array = result_array_filterd.filter(_._3 == 1.0)
        val class_b_array = result_array_filterd.filter(_._3 == 0.0)

        // println("array test --- a_length :" + class_a_array.length + " b_length :" + class_b_array.length)
        if(class_a_array.length >= class_b_array.length){
            println("Predict result:\nClass: 1")
            println("Probability:" + (class_a_array.length.toDouble / kValue))
            println("\nReal result:")
            println("Class: " + query(0)._19.toInt)
        }else{
            println("Predict result:\nClass: 0")
            println("Probability:" + (class_b_array.length.toDouble / kValue))
            println("\nReal result:")
            println("Class: " + query(0)._19.toInt)
        }
        // val result = sc.parallelize(result_array)
        // result.saveAsTextFile(outputPath) // Output
        sc.stop
    }
}
