/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.cli.test

import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import java.time.Instant

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

import org.junit.runner.RunWith

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.junit.JUnitRunner

import common.TestHelpers
import common.TestUtils._
import common.TestUtils
import common.WhiskProperties
import common.Wsk
import common.WskProps
import common.WskTestHelpers

/**
 * Tests for testing the CLI "api" subcommand.  Most of these tests require a deployed backend.
 */
@RunWith(classOf[JUnitRunner])
class ApiGwTests extends TestHelpers with WskTestHelpers with BeforeAndAfterEach with BeforeAndAfterAll {

  implicit val wskprops = WskProps()
  val wsk = new Wsk
  val clinamespace = wsk.namespace.whois()

  // This test suite makes enough CLI invocations in 60 seconds to trigger the OpenWhisk
  // throttling restriction.  To avoid CLI failures due to being throttled, track the
  // CLI invocation calls and when at the throttle limit, pause the next CLI invocation
  // with exactly enough time to relax the throttling.
  val maxActionsPerMin = WhiskProperties.getMaxActionInvokesPerMinute()
  val invocationTimes = new ArrayBuffer[Instant]()

  // Custom CLI properties file
  val cliWskPropsFile = File.createTempFile("wskprops", ".tmp")

  /**
   * Expected to be called before each test.
   * Assumes that each test will not invoke more than 5 actions and
   * settle the throttle when there isn't enough capacity to handle the test.
   */
  def checkThrottle(maxInvocationsBeforeThrottle: Int = maxActionsPerMin, expectedActivationsPerTest: Int = 5) = {
    val t = Instant.now
    val tminus60 = t.minusSeconds(60)
    val invocationsLast60Seconds = invocationTimes.filter(_.isAfter(tminus60)).sorted
    val invocationCount = invocationsLast60Seconds.length
    println(s"Action invokes within last minute: ${invocationCount}")

    if (invocationCount >= maxInvocationsBeforeThrottle) {
      // Instead of waiting a fixed 60 seconds to settle the throttle,
      // calculate a wait time that will clear out about half of the
      // current invocations (assuming even distribution) from the
      // next 60 second period.
      val oldestInvocationInLast60Seconds = invocationsLast60Seconds.head

      // Take the oldest invocation time in this 60 second period.  To clear
      // this invocation from the next 60 second period, the wait time will be
      // (60sec - oldest invocation's delta time away from the period end).
      // This will clear all of the invocations from the next period at the
      // expense of potentially waiting uncessarily long. Instead, this calculation
      // halves the delta time as a compromise.
      val throttleTime = 60.seconds.toMillis - ((t.toEpochMilli - oldestInvocationInLast60Seconds.toEpochMilli) / 2)
      println(s"Waiting ${throttleTime} milliseconds to settle the throttle")
      Thread.sleep(throttleTime)
    }

    invocationTimes += Instant.now
  }

  override def beforeEach() = {
    //checkThrottle()
  }

  /*
   * Create a CLI properties file for use by the tests
   */
  override def beforeAll() = {
    cliWskPropsFile.deleteOnExit()
    val wskprops = WskProps(token = "SOME TOKEN")
    wskprops.writeFile(cliWskPropsFile)
    println(s"wsk temporary props file created here: ${cliWskPropsFile.getCanonicalPath()}")
  }

  /*
   * Forcibly clear the throttle so that downstream tests are not affected by
   * this test suite
   */
  override def afterAll() = {
    // Check and settle the throttle so that this test won't cause issues with and follow on tests
    checkThrottle(30)
  }

  def apiCreate(basepath: Option[String] = None,
                relpath: Option[String] = None,
                operation: Option[String] = None,
                action: Option[String] = None,
                apiname: Option[String] = None,
                swagger: Option[String] = None,
                responsetype: Option[String] = None,
                expectedExitCode: Int = SUCCESS_EXIT,
                cliCfgFile: Option[String] = Some(cliWskPropsFile.getCanonicalPath()))(
    implicit wskpropsOverride: WskProps): RunResult = {

    checkThrottle()
    wsk.api.create(basepath, relpath, operation, action, apiname, swagger, responsetype, expectedExitCode, cliCfgFile)(
      wskpropsOverride)
  }

  def apiList(basepathOrApiName: Option[String] = None,
              relpath: Option[String] = None,
              operation: Option[String] = None,
              limit: Option[Int] = None,
              since: Option[Instant] = None,
              full: Option[Boolean] = None,
              nameSort: Option[Boolean] = None,
              expectedExitCode: Int = SUCCESS_EXIT,
              cliCfgFile: Option[String] = Some(cliWskPropsFile.getCanonicalPath())): RunResult = {

    checkThrottle()
    wsk.api.list(basepathOrApiName, relpath, operation, limit, since, full, nameSort, expectedExitCode, cliCfgFile)
  }

  def apiGet(basepathOrApiName: Option[String] = None,
             full: Option[Boolean] = None,
             expectedExitCode: Int = SUCCESS_EXIT,
             cliCfgFile: Option[String] = Some(cliWskPropsFile.getCanonicalPath()),
             format: Option[String] = None): RunResult = {

    checkThrottle()
    wsk.api.get(basepathOrApiName, full, expectedExitCode, cliCfgFile, format)
  }

  def apiDelete(basepathOrApiName: String,
                relpath: Option[String] = None,
                operation: Option[String] = None,
                expectedExitCode: Int = SUCCESS_EXIT,
                cliCfgFile: Option[String] = Some(cliWskPropsFile.getCanonicalPath())): RunResult = {

    checkThrottle()
    wsk.api.delete(basepathOrApiName, relpath, operation, expectedExitCode, cliCfgFile)
  }

  behavior of "Wsk api"

  it should "reject an api commands with an invalid path parameter" in {
    val badpath = "badpath"

    var rr = apiCreate(
      basepath = Some("/basepath"),
      relpath = Some(badpath),
      operation = Some("GET"),
      action = Some("action"),
      expectedExitCode = ANY_ERROR_EXIT)
    rr.stderr should include(s"'${badpath}' must begin with '/'")

    rr = apiDelete(
      basepathOrApiName = "/basepath",
      relpath = Some(badpath),
      operation = Some("GET"),
      expectedExitCode = ANY_ERROR_EXIT)
    rr.stderr should include(s"'${badpath}' must begin with '/'")

    rr = apiList(
      basepathOrApiName = Some("/basepath"),
      relpath = Some(badpath),
      operation = Some("GET"),
      expectedExitCode = ANY_ERROR_EXIT)
    rr.stderr should include(s"'${badpath}' must begin with '/'")
  }

  it should "reject an api commands with an invalid verb parameter" in {
    val badverb = "badverb"

    var rr = apiCreate(
      basepath = Some("/basepath"),
      relpath = Some("/path"),
      operation = Some(badverb),
      action = Some("action"),
      expectedExitCode = ANY_ERROR_EXIT)
    rr.stderr should include(s"'${badverb}' is not a valid API verb.  Valid values are:")

    rr = apiDelete(
      basepathOrApiName = "/basepath",
      relpath = Some("/path"),
      operation = Some(badverb),
      expectedExitCode = ANY_ERROR_EXIT)
    rr.stderr should include(s"'${badverb}' is not a valid API verb.  Valid values are:")

    rr = apiList(
      basepathOrApiName = Some("/basepath"),
      relpath = Some("/path"),
      operation = Some(badverb),
      expectedExitCode = ANY_ERROR_EXIT)
    rr.stderr should include(s"'${badverb}' is not a valid API verb.  Valid values are:")
  }

  it should "reject an api create command that specifies a nonexistent configuration file" in {
    val configfile = "/nonexistent/file"

    val rr = apiCreate(swagger = Some(configfile), expectedExitCode = ANY_ERROR_EXIT)
    rr.stderr should include(s"Error reading swagger file '${configfile}':")
  }

  it should "reject an api create command specifying a non-JSON configuration file" in {
    val file = File.createTempFile("api.json", ".txt")
    file.deleteOnExit()
    val filename = file.getAbsolutePath()

    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("a=A")
    bw.close()

    val rr = apiCreate(swagger = Some(filename), expectedExitCode = ANY_ERROR_EXIT)
    rr.stderr should include(s"Error parsing swagger file '${filename}':")
  }

  it should "reject an api create command specifying a non-swagger JSON configuration file" in {
    val file = File.createTempFile("api.json", ".txt")
    file.deleteOnExit()
    val filename = file.getAbsolutePath()

    val bw = new BufferedWriter(new FileWriter(file))
    bw.write("""|{
                    |   "swagger": "2.0",
                    |   "info": {
                    |      "title": "My API",
                    |      "version": "1.0.0"
                    |   },
                    |   "BADbasePath": "/bp",
                    |   "paths": {
                    |     "/rp": {
                    |       "get":{}
                    |     }
                    |   }
                    |}""".stripMargin)
    bw.close()

    val rr = apiCreate(swagger = Some(filename), expectedExitCode = ANY_ERROR_EXIT)
    rr.stderr should include(s"Swagger file is invalid (missing basePath, info, paths, or swagger fields")
  }

  it should "verify full list output" in {
    val testName = "CLI_APIGWTEST_RO1"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    try {
      println("cli namespace: " + clinamespace)
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      var rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      println("api create: " + rr.stdout)
      rr.stdout should include("ok: created API")
      rr = apiList(
        basepathOrApiName = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        full = Some(true))
      println("api list: " + rr.stdout)
      rr.stdout should include("ok: APIs")
      rr.stdout should include regex (s"Action:\\s+/${clinamespace}/${actionName}\n")
      rr.stdout should include regex (s"Verb:\\s+${testurlop}\n")
      rr.stdout should include regex (s"Base path:\\s+${testbasepath}\n")
      rr.stdout should include regex (s"Path:\\s+${testrelpath}\n")
      rr.stdout should include regex (s"API Name:\\s+${testapiname}\n")
      rr.stdout should include regex (s"URL:\\s+")
      rr.stdout should include(testbasepath + testrelpath)
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath)
    }
  }

  it should "verify successful creation and deletion of a new API" in {
    val testName = "CLI_APIGWTEST1"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path/with/sub_paths/in/it"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    try {
      println("cli namespace: " + clinamespace)

      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      var rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      rr = apiList(basepathOrApiName = Some(testbasepath), relpath = Some(testrelpath), operation = Some(testurlop))
      rr.stdout should include("ok: APIs")
      rr.stdout should include regex (s"/${clinamespace}/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath + testrelpath)
      rr = apiGet(basepathOrApiName = Some(testbasepath))
      rr.stdout should include regex (s""""operationId":\\s+"getPathWithSub_pathsInIt"""")
      val deleteresult = apiDelete(basepathOrApiName = testbasepath)
      deleteresult.stdout should include("ok: deleted API")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify get API name " in {
    val testName = "CLI_APIGWTEST3"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    try {
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      var rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      rr = apiGet(basepathOrApiName = Some(testapiname))
      rr.stdout should include(testbasepath)
      rr.stdout should include(s"${actionName}")
      rr.stdout should include regex (""""cors":\s*\{\s*\n\s*"enabled":\s*true""")
      rr.stdout should include regex (s""""target-url":\\s+.*${actionName}.json""")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify delete API name " in {
    val testName = "CLI_APIGWTEST4"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    try {
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      var rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      rr = apiDelete(basepathOrApiName = testapiname)
      rr.stdout should include("ok: deleted API")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify delete API basepath " in {
    val testName = "CLI_APIGWTEST5"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    try {
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      var rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      rr = apiDelete(basepathOrApiName = testbasepath)
      rr.stdout should include("ok: deleted API")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify adding endpoints to existing api" in {
    val testName = "CLI_APIGWTEST6"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path2"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    val newEndpoint = "/newEndpoint"
    try {
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      var rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(newEndpoint),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      rr = apiList(basepathOrApiName = Some(testbasepath))
      rr.stdout should include("ok: APIs")
      rr.stdout should include regex (s"/${clinamespace}/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath + testrelpath)
      rr.stdout should include(testbasepath + newEndpoint)
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify successful creation with swagger doc as input" in {
    // NOTE: These values must match the swagger file contents
    val testName = "CLI_APIGWTEST7"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    val swaggerPath = TestUtils.getTestApiGwFilename("testswaggerdoc1")
    try {
      var rr = apiCreate(swagger = Some(swaggerPath))
      rr.stdout should include("ok: created API")
      rr = apiList(basepathOrApiName = Some(testbasepath), relpath = Some(testrelpath), operation = Some(testurlop))
      println("list stdout: " + rr.stdout)
      println("list stderr: " + rr.stderr)
      rr.stdout should include("ok: APIs")
      // Actual CLI namespace will vary from local dev to automated test environments, so don't check
      rr.stdout should include regex (s"/[@\\w._\\-]+/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath + testrelpath)
    } finally {
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify adding endpoints to two existing apis" in {
    val testName = "CLI_APIGWTEST8"
    val testbasepath = "/" + testName + "_bp"
    val testbasepath2 = "/" + testName + "_bp2"
    val testrelpath = "/path2"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val testapiname2 = testName + " API Name 2"
    val actionName = testName + "_action"
    val newEndpoint = "/newEndpoint"
    try {
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      var rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      rr = apiCreate(
        basepath = Some(testbasepath2),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname2))
      rr.stdout should include("ok: created API")

      // Update both APIs - each with a new endpoint
      rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(newEndpoint),
        operation = Some(testurlop),
        action = Some(actionName))
      rr.stdout should include("ok: created API")
      rr = apiCreate(
        basepath = Some(testbasepath2),
        relpath = Some(newEndpoint),
        operation = Some(testurlop),
        action = Some(actionName))
      rr.stdout should include("ok: created API")

      rr = apiList(basepathOrApiName = Some(testbasepath))
      rr.stdout should include("ok: APIs")
      rr.stdout should include regex (s"/${clinamespace}/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath + testrelpath)
      rr.stdout should include(testbasepath + newEndpoint)

      rr = apiList(basepathOrApiName = Some(testbasepath2))
      rr.stdout should include("ok: APIs")
      rr.stdout should include regex (s"/${clinamespace}/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath2 + testrelpath)
      rr.stdout should include(testbasepath2 + newEndpoint)
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath2, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify successful creation of a new API using an action name using all allowed characters" in {
    // Be aware: full action name is close to being truncated by the 'list' command
    // e.g. /lime@us.ibm.com/CLI_APIGWTEST9a-c@t ion  is currently at the 40 char 'list' display max
    val testName = "CLI_APIGWTEST9"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "a-c@t ion"
    try {
      println("cli namespace: " + clinamespace)

      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      var rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      rr = apiList(basepathOrApiName = Some(testbasepath), relpath = Some(testrelpath), operation = Some(testurlop))
      rr.stdout should include("ok: APIs")
      rr.stdout should include regex (s"/${clinamespace}/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath + testrelpath)
      val deleteresult = apiDelete(basepathOrApiName = testbasepath)
      deleteresult.stdout should include("ok: deleted API")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify failed creation with invalid swagger doc as input" in {
    val testName = "CLI_APIGWTEST10"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    val swaggerPath = TestUtils.getTestApiGwFilename(s"testswaggerdocinvalid")
    try {
      val rr = apiCreate(swagger = Some(swaggerPath), expectedExitCode = ANY_ERROR_EXIT)
      println("api create stdout: " + rr.stdout)
      println("api create stderr: " + rr.stderr)
      rr.stderr should include(s"Swagger file is invalid")
    } finally {
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify delete basepath/path " in {
    val testName = "CLI_APIGWTEST11"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    try {
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      var rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      var rr2 = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testnewrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr2.stdout should include("ok: created API")
      rr = apiDelete(basepathOrApiName = testbasepath, relpath = Some(testrelpath))
      rr.stdout should include("ok: deleted " + testrelpath + " from " + testbasepath)
      rr2 = apiList(basepathOrApiName = Some(testbasepath), relpath = Some(testnewrelpath))
      rr2.stdout should include("ok: APIs")
      rr2.stdout should include regex (s"/${clinamespace}/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr2.stdout should include(testbasepath + testnewrelpath)
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify delete single operation from existing API basepath/path/operation(s) " in {
    val testName = "CLI_APIGWTEST12"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path2"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testurlop2 = "post"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    try {
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      var rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop2),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      rr = apiList(basepathOrApiName = Some(testbasepath))
      rr.stdout should include("ok: APIs")
      rr.stdout should include regex (s"/${clinamespace}/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath + testrelpath)
      rr = apiDelete(basepathOrApiName = testbasepath, relpath = Some(testrelpath), operation = Some(testurlop2))
      rr.stdout should include("ok: deleted " + testrelpath + " " + "POST" + " from " + testbasepath)
      rr = apiList(basepathOrApiName = Some(testbasepath))
      rr.stdout should include regex (s"/${clinamespace}/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify successful creation with complex swagger doc as input" in {
    val testName = "CLI_APIGWTEST13"
    val testbasepath = "/test1/v1"
    val testrelpath = "/whisk_system/utils/echo"
    val testrelpath2 = "/whisk_system/utils/split"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = "test1a"
    val swaggerPath = TestUtils.getTestApiGwFilename(s"testswaggerdoc2")
    try {
      var rr = apiCreate(swagger = Some(swaggerPath))
      println("api create stdout: " + rr.stdout)
      println("api create stderror: " + rr.stderr)
      rr.stdout should include("ok: created API")
      rr = apiList(basepathOrApiName = Some(testbasepath))
      rr.stdout should include("ok: APIs")
      // Actual CLI namespace will vary from local dev to automated test environments, so don't check
      rr.stdout should include regex (s"/[@\\w._\\-]+/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath + testrelpath)
      rr.stdout should include(testbasepath + testrelpath2)
    } finally {
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify successful creation and deletion with multiple base paths" in {
    val testName = "CLI_APIGWTEST14"
    val testbasepath = "/" + testName + "_bp"
    val testbasepath2 = "/" + testName + "_bp2"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val testapiname2 = testName + " API Name 2"
    val actionName = testName + "_action"
    try {
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      var rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname))
      rr.stdout should include("ok: created API")
      rr = apiList(basepathOrApiName = Some(testbasepath), relpath = Some(testrelpath), operation = Some(testurlop))
      rr.stdout should include("ok: APIs")
      rr.stdout should include regex (s"/${clinamespace}/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath + testrelpath)
      rr = apiCreate(
        basepath = Some(testbasepath2),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname2))
      rr.stdout should include("ok: created API")
      rr = apiList(basepathOrApiName = Some(testbasepath2), relpath = Some(testrelpath), operation = Some(testurlop))
      rr.stdout should include("ok: APIs")
      rr.stdout should include regex (s"/${clinamespace}/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath2 + testrelpath)
      rr = apiDelete(basepathOrApiName = testbasepath2)
      rr.stdout should include("ok: deleted API")
      rr = apiList(basepathOrApiName = Some(testbasepath), relpath = Some(testrelpath), operation = Some(testurlop))
      rr.stdout should include("ok: APIs")
      rr.stdout should include regex (s"/${clinamespace}/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath + testrelpath)
      rr = apiDelete(basepathOrApiName = testbasepath)
      rr.stdout should include("ok: deleted API")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath2, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "reject an API created with a non-existent action" in {
    val testName = "CLI_APIGWTEST15"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    try {
      val rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname),
        expectedExitCode = ANY_ERROR_EXIT)
      rr.stderr should include("does not exist")
    } finally {
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "reject an API created with an action that is not a web action" in {
    val testName = "CLI_APIGWTEST16"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    try {
      // Create the action for the API.  It must NOT be a "web-action" action for this test
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT)

      val rr = apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname),
        expectedExitCode = ANY_ERROR_EXIT)
      rr.stderr should include("is not a web action")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "verify API with http response type " in {
    val testName = "CLI_APIGWTEST17"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    val responseType = "http"
    try {
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname),
        responsetype = Some(responseType)).stdout should include("ok: created API")

      val rr = apiGet(basepathOrApiName = Some(testapiname))
      rr.stdout should include(testbasepath)
      rr.stdout should include(s"${actionName}")
      rr.stdout should include regex (s""""target-url":\\s+.*${actionName}.${responseType}""")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "reject API export when export type is invalid" in {
    val testName = "CLI_APIGWTEST18"
    val testbasepath = "/" + testName + "_bp"

    val rr = apiGet(basepathOrApiName = Some(testbasepath), format = Some("BadType"), expectedExitCode = ANY_ERROR_EXIT)
    rr.stderr should include("Invalid format type")
  }

  it should "successfully export an API in YAML format" in {
    val testName = "CLI_APIGWTEST19"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    val responseType = "http"
    try {
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname),
        responsetype = Some(responseType)).stdout should include("ok: created API")

      val rr = apiGet(basepathOrApiName = Some(testapiname), format = Some("yaml"))
      rr.stdout should include(s"basePath: ${testbasepath}")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "successfully export an API when JSON format is explcitly specified" in {
    val testName = "CLI_APIGWTEST20"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testnewrelpath = "/path_new"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"
    val responseType = "http"
    try {
      // Create the action for the API.  It must be a "web-action" action.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname),
        responsetype = Some(responseType)).stdout should include("ok: created API")

      val rr = apiGet(basepathOrApiName = Some(testapiname), format = Some("json"))
      rr.stdout should include(testbasepath)
      rr.stdout should include(s"${actionName}")
      rr.stdout should include regex (s""""target-url":\\s+.*${actionName}.${responseType}""")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "successfully create an API from a YAML formatted API configuration file" in {
    val testName = "CLI_APIGWTEST21"
    val testbasepath = "/bp"
    val testrelpath = "/rp"
    val testurlop = "get"
    val testapiname = testbasepath
    val actionName = "webhttpecho"
    val swaggerPath = TestUtils.getTestApiGwFilename(s"local.api.yaml")
    try {
      var rr = apiCreate(swagger = Some(swaggerPath))
      println("api create stdout: " + rr.stdout)
      println("api create stderror: " + rr.stderr)
      rr.stdout should include("ok: created API")
      rr = apiList(basepathOrApiName = Some(testbasepath))
      rr.stdout should include("ok: APIs")
      // Actual CLI namespace will vary from local dev to automated test environments, so don't check
      rr.stdout should include regex (s"/[@\\w._\\-]+/${actionName}\\s+${testurlop}\\s+${testapiname}\\s+")
      rr.stdout should include(testbasepath + testrelpath)
    } finally {
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "reject creation of an API from invalid YAML formatted API configuration file" in {
    val testName = "CLI_APIGWTEST22"
    val testbasepath = "/" + testName + "_bp"
    val swaggerPath = TestUtils.getTestApiGwFilename(s"local.api.bad.yaml")
    try {
      val rr = apiCreate(swagger = Some(swaggerPath), expectedExitCode = ANY_ERROR_EXIT)
      println("api create stdout: " + rr.stdout)
      println("api create stderror: " + rr.stderr)
      rr.stderr should include("Unable to parse YAML configuration file")
    } finally {
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "reject deletion of a non-existent api" in {
    val nonexistentApi = "/not-there"

    val rr = apiDelete(basepathOrApiName = nonexistentApi, expectedExitCode = ANY_ERROR_EXIT)
    rr.stderr should include(s"API '${nonexistentApi}' does not exist")
  }

  it should "successfully list an API whose endpoints are not mapped to actions" in {
    val testName = "CLI_APIGWTEST23"
    val testapiname = "A descriptive name"
    val testbasepath = "/NoActions"
    val testrelpath = "/"
    val testops: Seq[String] = Seq("put", "delete", "get", "head", "options", "patch", "post")
    val swaggerPath = TestUtils.getTestApiGwFilename(s"endpoints.without.action.swagger.json")

    try {
      var rr = apiCreate(swagger = Some(swaggerPath))
      println("api create stdout: " + rr.stdout)
      println("api create stderror: " + rr.stderr)
      rr.stdout should include("ok: created API")

      rr = apiList(basepathOrApiName = Some(testbasepath))
      println("api list:\n" + rr.stdout)
      testops foreach { testurlop =>
        rr.stdout should include regex (s"\\s+${testurlop}\\s+${testapiname}\\s+")
      }
      rr.stdout should include(testbasepath + testrelpath)

      rr = apiList(basepathOrApiName = Some(testbasepath), full = Some(true))
      println("api full list:\n" + rr.stdout)
      testops foreach { testurlop =>
        rr.stdout should include regex (s"Verb:\\s+${testurlop}")
      }
      rr.stdout should include(testbasepath + testrelpath)

    } finally {
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "reject creation of an API with invalid auth key" in {
    val testName = "CLI_APIGWTEST24"
    val testbasepath = "/" + testName + "_bp"
    val testrelpath = "/path"
    val testurlop = "get"
    val testapiname = testName + " API Name"
    val actionName = testName + "_action"

    try {
      // Create the action for the API.
      val file = TestUtils.getTestActionFilename(s"echo.js")
      wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))

      // Set an invalid auth key
      val badWskProps = WskProps(authKey = "bad-auth-key")

      apiCreate(
        basepath = Some(testbasepath),
        relpath = Some(testrelpath),
        operation = Some(testurlop),
        action = Some(actionName),
        apiname = Some(testapiname),
        expectedExitCode = ANY_ERROR_EXIT)(badWskProps).stderr should include("The supplied authentication is invalid")
    } finally {
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
      apiDelete(basepathOrApiName = testbasepath, expectedExitCode = DONTCARE_EXIT)
    }
  }

  it should "list api alphabetically by Base/Rel/Verb" in {
    val baseName = "/BaseTestPathApiList"
    val actionName = "actionName"
    val file = TestUtils.getTestActionFilename(s"echo-web-http.js")
    try {
      // Create Action for apis
      var action =
        wsk.action.create(name = actionName, artifact = Some(file), expectedExitCode = SUCCESS_EXIT, web = Some("true"))
      println("action creation: " + action.stdout)
      // Create apis
      for (i <- 1 to 3) {
        val base = s"$baseName$i"
        var api = apiCreate(
          basepath = Some(base),
          relpath = Some("/relPath"),
          operation = Some("GET"),
          action = Some(actionName))
        println("api creation: " + api.stdout)
      }
      val original = apiList(nameSort = Some(true)).stdout
      val originalFull = apiList(full = Some(true), nameSort = Some(true)).stdout
      val scalaSorted = List(s"${baseName}1" + "/", s"${baseName}2" + "/", s"${baseName}3" + "/")
      val regex = s"${baseName}[1-3]/".r
      val list = (regex.findAllMatchIn(original)).toList
      val listFull = (regex.findAllMatchIn(originalFull)).toList

      scalaSorted.toString shouldEqual list.toString
      scalaSorted.toString shouldEqual listFull.toString
    } finally {
      // Clean up Apis
      for (i <- 1 to 3) {
        apiDelete(basepathOrApiName = s"${baseName}$i", expectedExitCode = DONTCARE_EXIT)
      }
      wsk.action.delete(name = actionName, expectedExitCode = DONTCARE_EXIT)
    }
  }
}
