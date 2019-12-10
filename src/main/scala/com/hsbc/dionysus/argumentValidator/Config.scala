package com.hsbc.dionysus.argumentValidator

import java.nio.file.{Files, Paths}

import scopt.OptionParser

/** Case class for the config object.
 *
 * @param configurationFile The path to the JSON file with the configuration.
 */
case class Config(fileToProcess: String = "",
                  configurationFile: String = "",
                  sparkConfigs: Map[String, String] = Map())

/** Object that parses the arguments received. */
object Config {

  val parser: OptionParser[Config] =
    new scopt.OptionParser[Config](programName = "Spark") {
      head(xs = "Spark App")

      opt[String]('f', name = "fileToProcess")
        .valueName("fileToProcess.csv")
        .required()
        .action((x, c) => c.copy(configurationFile = x))
        .validate(x => if (isFileExists(filePath = x)) {
          success
        } else {
          failure(msg = "File to process does not exist")
        })
        .text("File to process")


      opt[String]('c', name = "configurationFile")
        .valueName("configuration_file.json")
        .required()
        .action((x, c) => c.copy(configurationFile = x))
        .validate(x => if (isFileExists(filePath = x)) {
          success
        } else {
          failure(msg = "JSON file does not exist")
        })
        .text("Service Config File")

      opt[Map[String, String]]('s', name = "sparkConfigs")
        .valueName("configs")
        .optional()
        .action((x, c) => c.copy(sparkConfigs = x))
        .text("configs")

    }


  /** Method to check if the json file received exists.
   *
   * @param filePath the json file to check.
   * @return true if the file exists, false if not.
   */
  def isFileExists(filePath: String): Boolean = {
    Files.exists(Paths.get(filePath))
  }

}
