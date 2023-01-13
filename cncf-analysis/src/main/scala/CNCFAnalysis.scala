package com.trinity

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

object CNCFAnalysis {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setAppName("Trinity-Demo").setMaster("local[*]")
    val spark = SparkSession.builder.appName("CNCF Application").getOrCreate()
    val sc = spark.sparkContext
    val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)

    val inputPath = "/tmp"
    val outputDir = args(0)
    val filePath = s"${inputPath}/interactive-landscape.csv*"

    val rawDf = spark.read.option("delimiter", ",").option("header", value = true).csv(filePath)

    val schema = Seq("Organization", "Total_Products", "Total_stars", "Latest_Commit_Date")

    val aggOrgDf = rawDf
      .na.drop("any", Array("Organization"))
      /*.toDF() .selectExpr(schema: _*).na.drop("any", Array("Organization"))*/
      .groupBy("Organization")
      .agg(
        countDistinct("Name").alias("Total_Products"),
        sum(col("Github Stars")).alias("Total_Stars"),
        max("Github Start Commit Date").alias("Latest_Commit_Date")
      )

    val finalDf = aggOrgDf.orderBy(desc("Total_Products")).selectExpr(schema: _*)

    finalDf
      .repartition(1)
      .write
      .mode("overwrite")
      .option("delimiter", "\t")
      .option("header", true)
      .csv(s"${inputPath}/result/")

    val filePart = fs
      .globStatus(new Path(s"$inputPath/result/part*"))(0).getPath.getName
    println(filePart)
    val fileToRename = new Path(s"$inputPath/result/$filePart")
    val newFileName = new Path(s"$inputPath/result/output.csv")
    val outPath = s"$inputPath/$outputDir/output.csv"
    println(outPath)
    fs.rename(fileToRename, newFileName)
  }

  private def aggregateOrganization(rawDf: DataFrame): DataFrame = {
    val aggDf = rawDf
      .groupBy("Organization")
      .agg(
        countDistinct("Name").alias("Total_Products"),
        functions.sum(col("Github Stars")).alias("Total_Stars"),
        functions.max("Github Start Commit Date").alias("Latest_Commit_Date")
      )
    aggDf.toDF()
  }
}
