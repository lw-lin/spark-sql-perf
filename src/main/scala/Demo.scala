/**
 * Copyright 2015 Tencent Inc.
 *
 * @author lwlin <lwlin@tencent.com>
 */

import com.databricks.spark.sql.perf.tpcds.Tables
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object Demo {
  def main(args: Array[String]) {
    val conf = new SparkConf
    conf.setMaster("local[4]")
    conf.setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dsdgenDir = "/Users/admin/Documents/ws_github/proflin/tpcds-kit/tools"
    val scaleFactor = 10

    // Tables in TPC-DS benchmark used by experiments.
    // dsdgenDir is the location of dsdgen tool installed in your machines.
    val tables = new Tables(sqlContext, dsdgenDir, scaleFactor)


    // Generate data.
    tables.genData(
      location = "/Users/admin/Desktop/tpcds_data_10g_parquet",
      format = "parquet",
      overwrite = true,
      partitionTables = false,
      useDoubleForDecimal = false,
      clusterByPartitionColumns = false,
      filterOutNullPartitionValues = false)


    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(
      location = "/Users/admin/Desktop/tpcds_data_10g_parquet",
      format = "parquet",
      databaseName = "mydb",
      overwrite = true)

    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    // Setup TPC-DS experiment
    import com.databricks.spark.sql.perf.tpcds.TPCDS
    val tpcds = new TPCDS(sqlContext = sqlContext)

  }

}
