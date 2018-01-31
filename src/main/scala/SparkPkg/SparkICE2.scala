package SparkPkg;
/**
  * Created by Mayanka on 09-Sep-15.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object SparkICE2 {

  def main(args: Array[String]) {

    //in windows needed this hadoop home
    System.setProperty("hadoop.home.dir","C:\\Users\\mraje_000\\Documents\\hadoopforspark");

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")

    val sc=new SparkContext(sparkConf)

    val input=sc.textFile("Spark WordCount\\input")

    val wc=input.flatMap(line=>{line.split(" ")}).distinct(); //transformations flatmap ,distinct
    val wc1=wc.map(word=>(word.charAt(0),word)).cache(); //transformations map FirstLetter of the Word, Word
    //val wc2 = wc1.sortByKey()

    val output=wc1.groupBy(word=>word._1).mapValues(_.map(_._2).mkString("(",",",")"))

    output.saveAsTextFile("Lab1\\ice2_output")

    val o=output.collect()

    var s:String="Words:group by first letter \n"
    o.foreach{case(firstletter,word)=>{

      s+=firstletter+" : "+word.toString()+"\n"

    }}
    print(s)
  }

}
