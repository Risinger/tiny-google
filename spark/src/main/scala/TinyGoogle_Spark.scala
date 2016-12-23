import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._

object TinyGoogle_Spark {
	def main(args: Array[String]) {

		val startTime = System.nanoTime;

		val conf = new SparkConf().setAppName("TinyGoogle_Spark")
		val sc = new SparkContext(conf)

		// val startCalcTime = System.nanoTime;

		val filesRDD = sc.wholeTextFiles("/Users/risinger/Documents/edu/CloudComputing/Project/books/*")
						 .map{case (path, text) => (path.substring(path.lastIndexOf("/")+1, path.length), text)}

		val file_wordsRDD = filesRDD.map{case (path, text) => (path, text.replaceAll("[^a-zA-Z0-9 -]+", "") //remove punctuation
													    	  .toLowerCase()
													    	  .split("\\s+") //split on any whitespace
													    	  .map(word => word.replaceAll("\\s+","")))} //remove whitespace

		val word_fileRDD = file_wordsRDD.flatMap(x => x._2.map(y => (y, x._1)))

		val word_file_countRDD = word_fileRDD.map(wf => (wf, 1)) //((word, file), 1) = init (non-unique) (word,file) counts to 1
											 .reduceByKey((x,y) => x+y) //add up words for word, file combinations
											 .map{case (word_file, count) => (word_file._1, (word_file._2, count))}
											 .groupByKey() //group by word
											 .map{case (word, file_count) => (word, file_count.toSeq.sortBy{case (file, count:Int) => -count})} //sort files by count

		word_file_countRDD.map{case (word, file_count) => word + "\t" + file_count.mkString("\t")} // create output strings
						  .repartition(1) //repatition for only 1 output file, though this will slow it down
						  .saveAsTextFile("/Users/risinger/Documents/edu/CloudComputing/Project/output/spark_output")

		// val stopCalcTime = System.nanoTime;
		sc.stop()
		val stopTime = System.nanoTime;

		val pw = new PrintWriter(new File("/Users/risinger/Documents/edu/CloudComputing/Project/output/spark_time.txt"))
		pw.write(((stopTime - startTime)/1e9d).toString + "\n")
		// pw.write(((stopCalcTime - startCalcTime)/1e9d).toString + "\n")
		pw.close()
	}
}