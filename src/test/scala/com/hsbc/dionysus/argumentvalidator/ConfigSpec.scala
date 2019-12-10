package com.hsbc.dionysus.argumentvalidator

import com.hsbc.dionysus.argumentValidator.Config
import org.scalatest.{Matchers, WordSpec}

class ConfigSpec extends WordSpec with Matchers {

  val wellFormedArguments: Seq[String] = Seq(
    "-f",
    getClass.getResource("/test_file.json").getPath,
    "-c",
    getClass.getResource("/test_file.json").getPath
  )

  val badKeyFileArguments: Seq[String] = Seq(
    "-f",
    getClass.getResource("/test_file.json").getPath,
    "-c",
    "non_existing_test_file.json"
  )

  "Config parser" should {
    "parse well formed arguments correctly" in {
      val actual: Option[Config] =
        Config.parser.parse(wellFormedArguments, Config())
      assert(actual.isDefined)
    }

    "fail when the JSON key file does not exist" in {
      val actual: Option[Config] =
        Config.parser.parse(badKeyFileArguments, Config())
      assert(actual.isEmpty)
    }
  }



}
