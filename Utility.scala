/**
  * Created by aguyyala on 6/21/17.
  */

import scala.collection.JavaConversions._
import org.apache.spark.sql.{SparkSession, DataFrame, Column, SaveMode}
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient
import scala.collection.JavaConverters._
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.apache.hadoop.fs.Options.Rename
import org.apache.hadoop.hive.metastore.api._
import org.apache.hadoop.hive.metastore.TableType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}


trait Utility {

    val log = Logger.getLogger(getClass)
    log.setLevel(Level.toLevel("Info"))


    val formatInfo = Map("orc" -> Map("serde" -> "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
        "inputFormat" -> "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        "outputFormat" -> "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"),
        "parquet" -> Map("serde" -> "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
            "inputFormat" -> "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "outputFormat" -> "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
        "csv" -> Map("serde" -> "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            "inputFormat" -> "org.apache.hadoop.mapred.TextInputFormat",
            "outputFormat" -> "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))


    /**
      * This module will handle  Skewness in the data and Small Files Problem.
      * While writing the dataframe to hdfs, it will create the number of files for each partition based on the passed map
      *
      * @param df: DataFrame to be Repartitioned
      * @param mainPartCol: Main Partitioning Column to define number of files
      * @param partCols: All the partitioning Columns
      * @param outNoFilesPerPartition: Map of outNoFiles Per Main Partition
      * @param defaultOutputNoFiles: Default Number of Output Files if the value is not found in passed Map
      * @return Repartitioned DataFrame along with Fake column to generate desired no of files
      *
      *
      * Eg:
      *
      * Call:
      * dataFrameOutputFiles(emailDF, country_code, Array("sendDate", "country_code"), Map("US" -> 160, "UK" -> 40), 10)
      *
      * Return's Repartitioned DataFrame that will Result in:
      * Country US -> Spark will produce 160 (specified in Map) files
      * Country UK -> Spark will produce 40 (specified in Map) files
      * Rest of the Countries -> Spark will produce 10 (defaultOutputNoFiles) files
      *
      */
    def dataFrameOutputFiles(df: DataFrame, mainPartCol: String, partCols: Seq[String], outNoFilesPerPartition: Map[String, Int], defaultOutputNoFiles: Int): DataFrame = {
        val fCol: Column = expr(outNoFilesPerPartition.map { case (part, numFiles) =>
            s" WHEN $mainPartCol = '$part' THEN floor(pmod(rand() * 100, $numFiles))"
        }.mkString("CASE\n", "\n", s"\nELSE floor(pmod(rand() * 100, $defaultOutputNoFiles)) END"))

        df.repartition(partCols.map(c => col(c)) :+ fCol: _*)
    }


    /**
      * Returns Hive equivalent DataType for Spark's DataType
      *
      * @param dataType: Spark's DataType
      * @return Hive's DataType
      */
    def sparktoHiveDataType(dataType: String) = {
        val decimalPattern = """DecimalType\(\d+,\s?\d+\)""".r

        dataType match {
            case "StringType" => "string"
            case "LongType" => "bigint"
            case "IntegerType" => "int"
            case "DateType" => "date"
            case "TimestampType" => "timestamp"
            case decimalPattern() => dataType.replace("Type", "").toLowerCase
            case _ => "string"
        }
    }


    /**
      * Mapping Input/Output formats from Hive to Spark
      */
    def getSparkFileFormat(fileFormat: String): String = {
        val format = fileFormat.toLowerCase

        format match {
            case _ if format contains "parquet" => "parquet"
            case _ if format contains "orc" => "orc"
        }

    }


    /**
      * Get Hive Table's Input Format
      * @param hiveMetaStore: HiveMetaStore
      * @param dbName: Database name
      * @param tableName: Table name
      * @return Hive Table's Input Format
      */
    def getHiveInputFormat(hiveMetaStore: HiveMetaStoreClient, dbName: String, tableName: String): String = {
        val hiveFormat = hiveMetaStore.getTable(dbName, tableName).getSd.getInputFormat
        getSparkFileFormat(hiveFormat)
    }


    /**
      * Save DataFrame to HDFS
      * @param df: DataFrame to be saved
      * @param path: HDFS Path to be saved to
      * @param format: Input Format
      * @param saveMode: SaveMode Overwrite OR Append
      */
    def saveDataFrameToHdfs(df: DataFrame, saveMode: SaveMode, format: String, path: String, partCols: Array[String] = Array()) = {
        log.info(s"Writing DataFrame to $path")
        if (format != "csv") {
            if (partCols.nonEmpty) df.write.mode(saveMode).format(format).partitionBy(partCols: _*).save(path)
            else df.write.mode(saveMode).format(format).save(path)
        }
        else {
            if (partCols.nonEmpty) df.write.mode(saveMode).format(format).option("sep", "\u0001").partitionBy(partCols: _*).save(path)
            else df.write.mode(saveMode).format(format).option("sep", "\u0001").save(path)
        }

    }


    /**
      * Extract Column and DataType(Hive's) Information from DataFrame
      * @param df: DataFrame whose column and datatype info to be extracted
      * @return Array((column, datatype(Hive)))
      */
    def getColsFromDF(df: DataFrame): Array[(String, String)] = {
        log.info("Extracting Column Info from DataFrame")
        df.dtypes.map { case (col, dtype) => (col, sparktoHiveDataType(dtype)) }
    }


    /**
      * Get HDFS Location of Hive Table
      *
      * @param hiveMetaStore: HiveMetaStore
      * @param dbName: Database name
      * @param tableName: Table name
      * @return HDFS Location of Hive Table
      */
    def getHiveTableLocation(hiveMetaStore: HiveMetaStoreClient, dbName: String, tableName: String): String = {
        hiveMetaStore.getTable(dbName, tableName).getSd.getLocation
    }


    /**
      * Alter Location of the Hive Table
      *
      * @param hiveMetaStore: HiveMetaStore
      * @param db: Database name
      * @param tbl: Table name
      * @param loc: New HDFS Location to be applied
      */
    def alterTableLocation(hiveMetaStore: HiveMetaStoreClient,db: String, tbl: String, loc: String) = {
        val newTbl = hiveMetaStore.getTable(db, tbl)
        val newSd = newTbl.getSd
        newSd.setLocation(loc)
        newTbl.setSd(newSd)
        hiveMetaStore.alter_table(db, tbl, newTbl)
    }


    /**
      * Checks whether passed DDL is identical to Hive Table's DDL
      *
      * @param hiveMetaStore: HiveMetaStore
      * @param dbName: Database name
      * @param tblName: Table name
      * @param cols: Column and Datatype Info, checks the Column Names, DataType Info, and Ordering of columns as well
      * @return True if matches else False
      */
    def checkDDL(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, cols: Seq[(String, String)], partCols: Seq[String] = Seq()): Boolean = {

        val tbl = hiveMetaStore.getTable(dbName, tblName)
        val hiveCols = tbl.getSd.getCols.asScala.map(x => (x.getName, x.getType)).toArray
        val hivePartCols = tbl.getPartitionKeys.asScala.map(x => (x.getName, x.getType)).toArray
        (cols.map(x => (x._1.toLowerCase, x._2.toLowerCase)) ++ partCols.map(x => (x, "string"))).toArray.deep == (hiveCols ++ hivePartCols).deep

    }


    /**
      * 1. Check's Table existence and create a new Table if it doesn't exist
      * 2. If Table exists, checks the DDL info and create the new Table if the existing Hive DDL is not mnatching with the passed DDL'
      *
      * @param hiveMetaStore: HiveMetaStore
      * @param dbName: Database name
      * @param tblName: Table name
      * @param format: Table's Input Format
      * @param location: HDFS Location of the Table
      * @param cols: Column and DataType info
      */
    def checkAndCreateHiveDDL(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, format: String,
                              location: String, cols: Seq[(String, String)], partCols: Seq[String] = Seq()) = {

        val tblExist = hiveMetaStore.tableExists(dbName, tblName)

        val ddlMatch = if (tblExist) checkDDL(hiveMetaStore, dbName, tblName, cols, partCols) else false

        if (!tblExist || !ddlMatch) {
            log.info("TABLE Does Not Exists OR DDL Mismatch, Creating New One")

            if (tblExist) hiveMetaStore.dropTable(dbName, tblName)

            val s = new SerDeInfo()
            s.setSerializationLib(formatInfo(format)("serde"))
            s.setParameters(Map("serialization.format" -> "1").asJava)

            val sd = new StorageDescriptor()
            sd.setSerdeInfo(s)
            sd.setInputFormat(formatInfo(format)("inputFormat"))
            sd.setOutputFormat(formatInfo(format)("outputFormat"))

            val tblCols = cols.map{case (c, dataType) => new FieldSchema(c, dataType, c)}.toList
            sd.setCols(tblCols.asJava)

            sd.setLocation(location)

            val t = new Table()

            t.setSd(sd)

            if (partCols.nonEmpty) {
                val tblPartCols = partCols.map(col => new FieldSchema(col, "string", col)).toList
                t.setPartitionKeys(tblPartCols.asJava)
            }

            t.setTableType(TableType.EXTERNAL_TABLE.toString)
            t.setDbName(dbName)
            t.setTableName(tblName)
            t.setParameters(Map("EXTERNAL" -> "TRUE", "tableType" -> "EXTERNAL_TABLE").asJava)

            hiveMetaStore.createTable(t)

        }
    }


    /**
      * Add Partition to Hive Table
      * @param hiveMetaStore: HiveMetatore
      * @param dbName: Database name
      * @param tblName: Table name
      * @param parts: Partitions to be added in BULK
      * @return Unit
      */
    def addHivePartitions(hiveMetaStore: HiveMetaStoreClient, dbName: String, tblName: String, parts: List[(List[String], String)]) = {

        val table = hiveMetaStore.getTable(dbName, tblName)
        val partitions = for (part <- parts) yield {
            val (p, loc) = part
            val partition = new Partition()
            partition.setDbName(dbName)
            partition.setTableName(tblName)
            val sd = new StorageDescriptor(table.getSd)
            sd.setLocation(loc)
            partition.setSd(sd)
            partition.setValues(p.asJava)
            partition
        }
        hiveMetaStore.add_partitions(partitions.asJava, true, true)
    }


    /**
      * Generate Quoted String from a Seq
      * @param seq: Seq to generate string from
      * @return Quoted String
      */
    def seqToQuotedString(seq: Seq[String]) = {
        seq.mkString("'", "', '", "'")
    }


    /**
      * Similar to "hdfs -mv" operation. Moves/Renames HDFS Source to Target Paths
      *
      * @param dfc: FileContext
      * @param srcPath: Source Path
      * @param tgtPath: Target Path
      */
    def hdfsMove(dfc: FileContext, srcPath: String, tgtPath: String): Unit = {
        log.info(s"Moving $srcPath to $tgtPath")
        dfc.rename(new Path(srcPath), new Path(tgtPath), Rename.OVERWRITE)
    }


    /**
      * Similar to "hdfs dfs -mv" except if the target path already exists it DOESN'T throw error
      * rather it deletes Target Path and Renames Source to Target Path
      *
      * @param dfs: FileSystem
      * @param dfc: FileContext
      * @param srcPath: Source Path
      * @param tgtPath: Target Path
      */
    def hdfsRemoveAndMove(dfs: FileSystem, dfc: FileContext, srcPath: String, tgtPath: String): Unit = {
        if (dfs.exists(new Path(tgtPath))) dfc.delete(new Path(tgtPath), true)
        dfs.mkdirs(new Path(tgtPath))
        hdfsMove(dfc, srcPath, tgtPath)
    }


    /**
      * Similar to "hdfs dfs -rm -r -skipTrash" operation. Deletes specified HDFS Location
      *
      * @param dfs; FileSystem
      * @param dfc: FileContext
      * @param path: HDFS Path to be deleted
      */
    def hdfsRemove(dfs: FileSystem, dfc: FileContext, path: String): Unit = {
        if (dfs.exists(new Path(path))) dfc.delete(new Path(path), true)
    }


    /**
      *Purge HDFS Path based on Retention Policy
      * @param dfs: FileSystem
      * @param dfc: FileContext
      * @param loc: HDFS Location to be purged
      * @param rententionDays: Retention Days
      */
    def purgeHDFSPath(dfs: FileSystem, dfc: FileContext, loc: String, rententionDays: String) = {
        dfs.listStatus(new Path(loc))
            .filter( _.getPath.toString.split("/").last < rententionDays)
            .map(_.getPath.toString).foreach(hdfsRemove(dfs, dfc, _))
    }


    /**
      * Creates RDD CheckPoint to cut down Lineage Graph and frees up Driver's Memory
      *
      * Note: CheckPoint writes entire Serialized RDD to the specified Checkpoint Directory, please be aware of the RDD size
      *
      * Additional Advantage: If the some of the executors get's Lost/Preempted after the checkpoint, it reads the RDD
      * from disk, saves time without re-computing it
      *
      * @param spark: SparkSession
      * @param df: DataFrame to be CheckPointed
      * @param msg: Message to be displayed for the action Count
      * @return New Checkpointed DataFrame
      */
    def createCheckPoint(spark: SparkSession, df: DataFrame, msg: String = "") = {
        df.rdd.checkpoint
        val dfCount = df.rdd.count

        if (msg != "") log.info(s"Logging Count - $msg - $dfCount")
        else log.info(s"Checkpoint Count - $dfCount")

        spark.createDataFrame(df.rdd, df.schema)
    }


    /**
      * Fetches data from SQL Server to Spark
      *
      * @param spark: SparkSession
      * @param edwSqlText: Query to be fetched
      * @return DataFrame
      */
    def getData(spark: SparkSession, edwSqlText: String) = {
        spark
            .read
            .option("user", spark.conf.get("user"))
            .option("password", spark.conf.get("password"))
            .option("url", spark.conf.get("url"))
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .format("jdbc")
            .option("dbtable", s"($edwSqlText) as src")
            .load
    }


    /**
      * Get's the Primary Key (Ignore's Data Columns) of the table in SQL Server - Used for partitioning the dataset
      *
      * @param spark: SparkSession
      * @param db: SQL Server's Database
      * @param tbl: SQL Server's Table
      * @return Primary Key to be used for partitioning the dataset
      */
    def getPrimaryKey(spark: SparkSession, db: String, tbl: String) = {
        val query =
            s"""
               |SELECT column_name as PRIMARYKEYCOLUMN
               |FROM $db.INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC
               |INNER JOIN
               |    $db.INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS KU
               |          ON TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND
               |             TC.CONSTRAINT_NAME = KU.CONSTRAINT_NAME AND
               |             KU.table_name='$tbl'
     """.stripMargin

        getData(spark, query).collect().filterNot(_ (0).toString.toLowerCase contains "date")(0)(0).toString.toLowerCase
    }


    /**
      * Fetches Data from SQL Server using Multiple Threads
      *
      * @param spark; SparkSession
      * @param sqlQuery: Query to be fetched
      * @param numPartitions: Number of threads
      * @return DataFrame
      */
    def getEDWData(spark: SparkSession, sqlQuery: String, numPartitions: Int = 20) = {

        var query = sqlQuery.replaceAll("\\s+", " ")

        val (db, schema, table) = """(?i) FROM ([\w.]+)""".r.findAllIn(query).matchData.map(_.group(1)).toList.head.split("\\.") match {
            case Array(a, b, c) => (a, b, c)
        }

        val partitionColumnName = getPrimaryKey(spark, db, table)

        val boundQueryDf = getData(spark, s"SELECT MIN($partitionColumnName) min, MAX($partitionColumnName) max FROM $db.$schema.$table").collect()(0)

        val (lowerBound, upperBound) = (boundQueryDf.get(0).toString.toLong, boundQueryDf.get(1).toString.toLong)

        val cols = """(?i)SELECT (.*?) FROM """.r.findAllIn(query).matchData.map(_.group(1)).toList.head.split(",").map(_.trim).map(_.toLowerCase).toSet

        if (!cols.contains(partitionColumnName) && cols.mkString(",") != "*") {
            query = query.replaceFirst("(?i)SELECT ", "SELECT " + partitionColumnName + ", ")
        }

        spark.read
            .format("jdbc")
            .option("user", spark.conf.get("user"))
            .option("password", spark.conf.get("password"))
            .option("url", spark.conf.get("url"))
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("partitionColumn", partitionColumnName)
            .option("lowerBound", lowerBound)
            .option("upperBound", upperBound)
            .option("numPartitions", numPartitions)
            .option("dbtable", String.format("(%s) as src", query))
            .load
    }


    /** Returns DataFrame with new nullability constraints from given schema
      *
      * @param spark  = source Spark Session
      * @param df     = source DataFrame
      * @param schema = schema with nullability constraints to be enforced on source DataFrame
      */
    def enforceSchemaNullability(spark: SparkSession, df: DataFrame, schema: StructType): DataFrame = {
        //ensures schema field names are only entered once
        val colCount: Int = schema.map(x => (x.name, x.nullable)).groupBy(_._1).mapValues(_.length).values.reduce(math.max)
        assert(colCount == 1, "Enforced schema has more than one field with the same name!")

        val nullabilityMap: Map[String, Boolean] = schema.map(x => (x.name.toLowerCase, x.nullable)).toMap
        val newFields = df.schema.map(x => x.copy(nullable = nullabilityMap(x.name.toLowerCase)))
        val newSchema = StructType(newFields)
        spark.createDataFrame(df.rdd, newSchema)
    }


    /** Returns DataFrame with new column types casted from given schema
      *
      * @param df     = source DataFrame
      * @param schema = new schema to which the source DataFrame will have its columns casted to align with
      */
    def enforceSchemaTypes(df: DataFrame, schema: StructType): DataFrame = {
        //ensures schema field names are only entered once
        val colCount: Int = schema.map(x => (x.name, x.dataType)).groupBy(_._1).mapValues(_.length).values.reduce(math.max)
        assert(colCount == 1, "Enforced schema has more than one field with the same name!")

        val typeMap: Map[String, DataType] = schema.map(x => (x.name.toLowerCase, x.dataType)).toMap
        df.columns.foldLeft(df) { (z, col) => z.withColumn(col, z(col).cast(typeMap(col.toLowerCase))) }
    }


    /** Returns DataFrame with updated rows
      *
      * DataFrames cannot be defined from others using a Spark SQL UPDATE statement.  This function allows
      * the caller to supply an UPDATE-like statement and the DataFrame to be updated and will merge the updated rows
      * returned by the query with those not selected for update.
      *
      * @param spark      = SparkSession object
      * @param df         = DataFrame from which new, updated rows will be generated from
      * @param dfViewName = the view created for the df
      * @param query      = String which will query the passed df to generate new rows that can be unioned with the old, unmodified rows
      *
      * Example:
      *
      * // Sample DataFrame
      * case class BrandMap(MasterBrandID:Int, DisplayBrandID:Int)
      * val dfBaseBrandExpansion = Seq(
      *                                 BrandMap(121,122)
      *                                 ,BrandMap(121,123)
      *                                 ,BrandMap(121,124)
      *                                 ,BrandMap(128,131)
      *                                 ,BrandMap(128,132)
      *                                 ,BrandMap(130,133)
      *                                 ,BrandMap(611,624)
      *                                 ,BrandMap(651,656)
      *                               ).toDF
      *
      * // Create view for this sample
      * dfBaseBrandExpansion.createOrReplaceTempView("dfBaseBrandExpansion_VIEW")
      *
      *
      * // TRADITIONAL UPDATE STATEMENT
      *
      * UPDATE dfBaseBrandExpansion_VIEW
      * SET MasterBrandID = MasterBrandID +1
      * WHERE MasterBrandID < 600
      *
      *
      * // We change the UPDATE statement to select all columns from the source DataFrame
      * // and add updated columns aliased as the original columns with a "_TMP" suffix.
      * // Newly updated rows will be created and merged with the unmodified rows.
      *
      * val updateStatement = """
      *                         |SELECT be.*
      *                         |  ,MasterBrandID +1 AS MasterBrandID_TMP
      *                         |FROM {SOURCE_DF} AS be
      *                         |WHERE MasterBrandID < 600
      *                         |""".stripMargin
      *
      * val dfNew = updateDataFrame(spark, dfBaseBrandExpansion, "dfBaseBrandExpansion_VIEW", updateStatement)
      *
      * dfBaseBrandExpansion.show
      *
      * +-------------+--------------+
      * |MasterBrandID|DisplayBrandID|
      * +-------------+--------------+
      * |          121|           122|
      * |          121|           123|
      * |          121|           124|
      * |          128|           131|
      * |          128|           132|
      * |          130|           133|
      * |          611|           624|
      * |          651|           656|
      * +-------------+--------------+
      *
      *
      * dfNew.show
      *
      * +-------------+--------------+
      * |MasterBrandID|DisplayBrandID|
      * +-------------+--------------+
      * |          122|           122|
      * |          122|           123|
      * |          122|           124|
      * |          129|           131|
      * |          129|           132|
      * |          131|           133|
      * |          611|           624|
      * |          651|           656|
      * +-------------+--------------+
      *
      *
      * // ListingDisplay table
      * val dflistingDisplay = spark.sql("SELECT * FROM eagle_edw_batch.ListingDisplay")
      * dflistingDisplay.createOrReplaceTempView("dflistingDisplay_VIEW")
      *
      *
      * val incrementQuery = """
      *                         |SELECT ld.*
      *                         |  ,listingid + 1 AS listingid_TMP
      *                         |
      *                         |FROM {SOURCE_DF} AS ld
      *                         """.stripMargin
      *
      *
      *
      * val incrementedListingDisplay = updateDataFrame(spark, dflistingDisplay, "dflistingDisplay_VIEW", incrementQuery)
      */
    def updateDataFrame(spark: SparkSession, df: DataFrame, dfViewName: String, query: String, msg: String = ""): DataFrame = {

        // Ensure query contains the substring "{SOURCE_DF}"
        val queryDFKeyword = "{SOURCE_DF}"
        assert(query.contains(queryDFKeyword), s"Updating query does not contain $queryDFKeyword!")

        // Add a unique row identifier to the DataFrame being updated and create a corresponding view
        val rowIdColName = "tmpRowId"
        val oldDFWithIds = df.withColumn(rowIdColName, monotonically_increasing_id())

        // Create view atop the source DataFrame with a rowid field
        val oldViewName = "oldDFWithIds_VIEW"
        oldDFWithIds.createOrReplaceTempView(oldViewName)

        // Inject the view with rowids into the supplied query
        val viewAdjustedQuery = query.replaceAllLiterally(queryDFKeyword, oldViewName)

        // Get updated rows
        val newRowsWithTMPCols = spark.sql(viewAdjustedQuery)

        // Find new "_TMP" columns
        val tmpRegEx = "\\_TMP$".r
        val tmpCols = newRowsWithTMPCols.columns.filter(_.endsWith("_TMP"))
        assert(tmpCols.length > 0, s"No new columns defined in the update to $dfViewName!")

        // Rename the TMP columns in the updated rows to their proper names
        var newRowsWithRenamedTMPCols = tmpCols.foldLeft(newRowsWithTMPCols) {
            (z, col) => {
                val cleanedCol = tmpRegEx.replaceAllIn(col, "")
                z.withColumn(cleanedCol, z(col)).drop(col)
            }
        }

        val checkExtraCols = newRowsWithRenamedTMPCols.columns.map(_.toLowerCase).toSet -- oldDFWithIds.columns.map(_.toLowerCase).toSet

        if (checkExtraCols.nonEmpty) {
            newRowsWithRenamedTMPCols = newRowsWithRenamedTMPCols.drop(checkExtraCols.toList: _*)
        }

        // Enforce the old schema on the new df
        val updatedNullabilityRows = enforceSchemaNullability(spark, newRowsWithRenamedTMPCols, oldDFWithIds.schema)
        val newDFWithIds = enforceSchemaTypes(updatedNullabilityRows, oldDFWithIds.schema)

        val newViewName = "newDFWithIds_VIEW"
        newDFWithIds.createOrReplaceTempView(newViewName)

        newDFWithIds.persist
        val updatedCount = newDFWithIds.count

        if (msg != "") log.info(s"Logging Count - $msg - $updatedCount")
        else log.info(s"UPDATED ROWS Count - $updatedCount")

        val cols = oldDFWithIds.columns
            .filterNot(_ == rowIdColName)
            .map(x => s"CASE WHEN B.$rowIdColName IS NOT NULL THEN B.$x ELSE A.$x END AS $x")
            .mkString(",")

        val joinquery = s"SELECT $cols FROM $oldViewName A LEFT JOIN $newViewName B ON A.$rowIdColName = B.$rowIdColName"

        val finalDF = spark.sql(joinquery).repartition(50)
        finalDF.persist
        val totalCount = finalDF.count
        log.info(s"TOTAL ROWS Count After the Update Operation: $totalCount")

        newDFWithIds.unpersist
        finalDF
    }

}

