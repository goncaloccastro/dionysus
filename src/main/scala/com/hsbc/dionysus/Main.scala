package com.hsbc.dionysus

import java.util.Properties

import com.hsbc.centaur.Core
import com.hsbc.dionysus.argumentValidator.Config
import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.sql.{DataFrame, SparkSession}
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

object Main extends App with StrictLogging {

  logger.info("Starting Spark Application")
  val config: Config = Config.parser.parse(args, Config()) match {
    case Some(config) => config
    case None => throw new IllegalArgumentException()
  }
  logger.info("All input arguments were correctly parsed")

  case class ApplicationConfiguration(sparkAppName: String, databaseURL: String, tableName: String)

  object ApplicationConfigurationJsonSupport extends DefaultJsonProtocol {
    implicit val applicationConfigurationFormat: RootJsonFormat[ApplicationConfiguration] =
      jsonFormat3(ApplicationConfiguration)
  }

  import ApplicationConfigurationJsonSupport._
  import spray.json._

  val applicationConfiguration: ApplicationConfiguration =
    Core.getFileContents(filePath = config.configurationFile).get.parseJson.convertTo[ApplicationConfiguration]

  val sparkSession: SparkSession = createSparkSession(
    configs = config.sparkConfigs,
    sparkAppName = applicationConfiguration.sparkAppName
  )

  val sourceDataFrame = readSourceFile(sparkSession = sparkSession, sourceFilePath = config.fileToProcess)

  saveToDataBase(dataFrame = sourceDataFrame, applicationConfiguration = applicationConfiguration)

  def readSourceFile(sparkSession: SparkSession, sourceFilePath: String): DataFrame = {
    sparkSession.read.csv(sourceFilePath)
  }

  def saveToDataBase(dataFrame: DataFrame, applicationConfiguration: ApplicationConfiguration, connectionProperties: Properties = new Properties()): Unit = {
    dataFrame.write.jdbc(
      applicationConfiguration.databaseURL,
      s"schema.${applicationConfiguration.tableName}",
      connectionProperties)
  }

  /**
   * Creates a spark session using the (optional) provided configurations.
   *
   * Should the configurations be empty, the default spark session will be returned.
   *
   * @param configs (optional) provided configurations.
   * @return a configured spark session.
   */
  def createSparkSession(configs: Map[String, String], sparkAppName: String): SparkSession = {
    logger.info(s"Creating Spark Session with name $sparkAppName")
    val builder =
      SparkSession.builder()
        .appName(sparkAppName)

    Some(
      configs.foldLeft(builder)((accum, configs) => accum.config(configs._1, configs._2))
    ).getOrElse(builder)
      .getOrCreate()
  }
}
