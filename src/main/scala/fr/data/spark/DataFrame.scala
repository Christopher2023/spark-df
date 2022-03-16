package fr.data.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataFrame {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()  
    .appName("job-1")  
    .master("local[2]")  
    .getOrCreate()
    
    val df = spark.read.format("csv")
    .option("header","true")
    .option("delimiter", ";")  
    .option("inferSchema", "true")
    .load("src/main/resources/codesPostaux.csv")
    df.show()

    df.printSchema

    df.select(countDistinct("Code_commune_INSEE")).show()

    df.filter(col("Ligne_5").isNotNull).select(countDistinct("Code_commune_INSEE")).show()

    val new_df = df.withColumn("Departement", col("Code_commune_INSEE").substr(1,2))
    new_df.show()

    val com_dep = new_df.select(col("Code_commune_INSEE"),col("Nom_commune"),col("Code_postal"),col("Departement")).orderBy(asc("Code_postal"))
    com_dep.write.format("csv")
    .option("header",true)
    .option("delimiter",",")
    .mode("overwrite")
    .csv("src/main/resources/commune_et_departement.csv")

    new_df.filter(col("Departement") === 02).select(col("Code_commune_INSEE"),col("Nom_commune")).distinct().show()

    val inter = new_df.groupBy(col("Code_commune_INSEE")).countDistinct()
    inter.select(max(col("count"))).show()
  }


}
