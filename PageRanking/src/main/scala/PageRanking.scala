import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext


object PageRanking {
  def main(args: Array[String]): Unit = {
    //if running on windows machine - set homedir to where /bin/winutils.exe
//    System.setProperty("hadoop.home.dir", "C:\\hadoop\\")
    if (args.length != 3) {
      println("Usage: Iteration InputFilePath OutputDir")
    }
    // create Spark context with Spark configuration

    val spark = SparkSession.builder
      //change master to local[<no_of_cores>] for local testing
//      .master("local[4]")
      .master("yarn")
      .appName("Page Ranking")
      .getOrCreate()
    import spark.implicits._

    var iteration= args(1).toInt
    val inputDF =spark.read.option("header","true").option("inferSchema","true").csv(args(0)).select($"ORIGIN",$"DEST")
    val  inputAgg=inputDF.groupBy("ORIGIN","DEST").count().orderBy(desc("count")).withColumnRenamed("count","links")
    var PageRanks=inputAgg.select($"Dest".alias("Airport")).distinct().withColumn("PageRank", lit(10))
    PageRanks.cache()
    val outDegrees=inputAgg.groupBy($"ORIGIN").agg(
      sum("links").as("outDegree"))
    outDegrees.cache()
    val totalNodes=spark.sparkContext.broadcast(PageRanks.count())
    var StagingTable=inputAgg.join(PageRanks,$"ORIGIN" === $"AIRPORT").join(outDegrees,Seq("ORIGIN")).select($"ORIGIN",$"PageRank".alias("OriginPageRank"),$"outDegree".alias("OriginOutlinks"),$"DEST").withColumn("PrUponC", ($"OriginPageRank" / $"OriginOutlinks")*0.85) //alpa=0.15

    var StagingTableRDD=StagingTable.rdd.map(x=>(x.getAs[String]("DEST"),x.getAs[Double]("PrUponC")))
    var PageRanksRDD=StagingTableRDD.reduceByKey((x,y)=> x+ y).map{case(x,y)=>(x,y+(0.15/totalNodes.value))}.sortBy(-_._2)
    PageRanks=PageRanksRDD.toDF("Airport","PageRank")
    PageRanks.cache()
    iteration=iteration-1
    if(iteration>0){

      while(iteration >0){
        StagingTable=inputAgg.join(PageRanks,$"ORIGIN" === $"AIRPORT").join(outDegrees,Seq("ORIGIN")).select($"ORIGIN",$"PageRank".alias("OriginPageRank"),$"outDegree".alias("OriginOutlinks"),$"DEST").withColumn("PrUponC", ($"OriginPageRank" / $"OriginOutlinks")*0.85)
        StagingTableRDD=StagingTable.rdd.map(x=>(x.getAs[String]("DEST"),x.getAs[Double]("PrUponC")))
        PageRanksRDD=StagingTableRDD.reduceByKey((x,y)=> x+ y).map{case(x,y)=>(x,y+(0.15/totalNodes.value))}.sortBy(-_._2)
        PageRanks=PageRanksRDD.toDF("Airport","PageRank")
        PageRanks.cache()
        println(s"iteration $iteration completed..")
        iteration=iteration-1
      }
    }
    PageRanksRDD.coalesce(1, shuffle = true).saveAsTextFile(args(2))

  }

}
