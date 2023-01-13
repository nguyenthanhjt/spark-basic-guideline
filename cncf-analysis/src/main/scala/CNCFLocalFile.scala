package com.spark

import org.apache.spark.sql.functions.{col, countDistinct, desc, sum}
import org.apache.spark.sql.{SparkSession, functions}

import java.io.File
import java.nio.file.{Files, Paths}

/**
 * An implementation for CNCF Analysis with file on local hard-drive
 */
object CNCFLocalFile {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CNCF Application").getOrCreate()

    var folderPath = args(0)
    var fileName = args(1)

    var filePath = s"$folderPath/$fileName"

    val schema = Seq("Organization", "Total_Products", "Total_stars", "Latest_Commit_Date")

    var rawDf = spark.read
      .option("delimiter", ",")
      .option("header", value = true)
      .csv(filePath)
      .na.drop("any", Array("Organization"))
      .filter(col("Name").isNotNull)

    var aggDf = rawDf
      .groupBy("Organization")
      .agg(
        countDistinct("Name").alias("Total_Products"),
        sum(col("Github Stars")).alias("Total_stars"),
        functions.max(col("Github Start Commit Date")).alias("Latest_Commit_Date")
      )

    val finalDf = aggDf.orderBy(desc("Total_Products")).selectExpr(schema: _*)

    var outputFolder = s"$folderPath/result/"
    finalDf
      .repartition(1)
      .write
      .mode("overwrite")
      .option("delimiter", "\t")
      .option("header", value = true)
      .csv(outputFolder)


    val fileList = getListOfFiles(outputFolder)

    val outputFileName = getRawFilePath(outputFolder)

    println(s"Raw output path:$outputFileName")
    renameFile(outputFileName, outputFolder + "cncf-output.csv")
    println(s"Renamed file name:$outputFileName")

  }

  private def getListOfFiles(dir: String): List[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.contains("part*.csv"))
      .map(_.getPath).toList
  }

  private def getRawFilePath(dir: String): String = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile)
      .filter(_.getName.startsWith("part"))
      .map(_.getPath).toList.head
  }

  private def renameFile(oldPath: String, newPath: String) = {
    val oldFile = Paths.get(oldPath)
    val newFile = Paths.get(newPath)
    Files.move(oldFile, newFile)
  }

}
