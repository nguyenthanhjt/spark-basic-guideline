import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._

val rawDf = spark.read.option("delimiter",",").option("header",value=true)
  .csv("file:///C:/interactive-landscape.csv")

val aggOrgDf = rawDf.na
  .drop("any", Array("Organization"))
  .groupBy("Organization")
  .agg(
    countDistinct("Name").alias("Total_Products"),
    sum(col("Github Stars")).alias("Total_stars"),
    max("Github Start Commit Date").alias("Latest_Commit_Date")
  )
  .selectExpr(
    Seq(
      "Organization",
      "Total_Products",
      "Total_stars",
      "Lastest_Commit_Date"
    ): _*
  )
  .orderBy(desc("Total_Products"))

val finalDf = aggOrgDf.orderBy(desc("Total_Products")) // .selectExpr(schema: _*)

finalDf
  .repartition(1)
  .write
  .mode("overwrite")
  .option("delimiter", "\t")
  .option("header", true)
  .csv("file:///C:/cncf-output.csv")
