/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.resource

import java.io.File
import java.nio.file.{Files, Paths}

import scala.util.control.NonFatal

import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils.executeAndGetOutput

/**
 * 资源标识符。
 * @param componentName spark.driver / spark.executor / spark.task
 * @param resourceName  gpu, fpga, etc
 */
private[spark] case class ResourceID(componentName: String, resourceName: String) {
  def confPrefix: String = s"$componentName.resource.$resourceName." // with ending dot
  def amountConf: String = s"$confPrefix${ResourceUtils.AMOUNT}"
  def discoveryScriptConf: String = s"$confPrefix${ResourceUtils.DISCOVERY_SCRIPT}"
  def vendorConf: String = s"$confPrefix${ResourceUtils.VENDOR}"
}

private[spark] case class ResourceRequest(
    id: ResourceID,
    amount: Int,
    discoveryScript: Option[String],
    vendor: Option[String])

private[spark] case class ResourceRequirement(resourceName: String, amount: Int)

/**
 * Case class representing allocated resource addresses for a specific resource.
 * Cluster manager uses the JSON serialization of this case class to pass allocated resource info to
 * driver and executors. See the ``--resourcesFile`` option there.
 */
private[spark] case class ResourceAllocation(id: ResourceID, addresses: Seq[String]) {
  def toResourceInformation: ResourceInformation = {
    new ResourceInformation(id.resourceName, addresses.toArray)
  }
}

private[spark] object ResourceUtils extends Logging {
  // config suffixes
  val DISCOVERY_SCRIPT = "discoveryScript"
  val VENDOR = "vendor"
  // user facing configs use .amount to allow to extend in the future,
  // internally we currently only support addresses, so its just an integer count
  val AMOUNT = "amount"

  def parseResourceRequest(sparkConf: SparkConf, resourceId: ResourceID): ResourceRequest = {
    val settings = sparkConf.getAllWithPrefix(resourceId.confPrefix).toMap
    val amount = settings.getOrElse(AMOUNT,
      throw new SparkException(s"You must specify an amount for ${resourceId.resourceName}")
    ).toInt
    val discoveryScript = settings.get(DISCOVERY_SCRIPT)
    val vendor = settings.get(VENDOR)
    ResourceRequest(resourceId, amount, discoveryScript, vendor)
  }

  def listResourceIds(sparkConf: SparkConf, componentName: String): Seq[ResourceID] = {
    sparkConf.getAllWithPrefix(s"$componentName.resource.").map { case (key, _) =>
      key.substring(0, key.indexOf('.'))
    }.toSet.toSeq.map(name => ResourceID(componentName, name))
  }

  def parseAllResourceRequests(
      sparkConf: SparkConf,
      componentName: String): Seq[ResourceRequest] = {
    listResourceIds(sparkConf, componentName).map { id =>
      parseResourceRequest(sparkConf, id)
    }
  }

  def parseResourceRequirements(sparkConf: SparkConf, componentName: String)
    : Seq[ResourceRequirement] = {
    parseAllResourceRequests(sparkConf, componentName).map { request =>
      ResourceRequirement(request.id.resourceName, request.amount)
    }
  }

  def resourcesMeetRequirements(
      resourcesFree: Map[String, Int],
      resourceRequirements: Seq[ResourceRequirement])
    : Boolean = {
    resourceRequirements.forall { req =>
      resourcesFree.getOrElse(req.resourceName, 0) >= req.amount
    }
  }

  def withResourcesJson[T](resourcesFile: String)(extract: String => Seq[T]): Seq[T] = {
    val json = new String(Files.readAllBytes(Paths.get(resourcesFile)))
    try {
      extract(json)
    } catch {
      case NonFatal(e) =>
        throw new SparkException(s"Error parsing resources file $resourcesFile", e)
    }
  }

  def parseAllocatedFromJsonFile(resourcesFile: String): Seq[ResourceAllocation] = {
    withResourcesJson[ResourceAllocation](resourcesFile) { json =>
      implicit val formats = DefaultFormats
      parse(json).extract[Seq[ResourceAllocation]]
    }
  }

  private def parseAllocatedOrDiscoverResources(
      sparkConf: SparkConf,
      componentName: String,
      resourcesFileOpt: Option[String]): Seq[ResourceAllocation] = {
    val allocated = resourcesFileOpt.toSeq.flatMap(parseAllocatedFromJsonFile)
      .filter(_.id.componentName == componentName)
    val otherResourceIds = listResourceIds(sparkConf, componentName).diff(allocated.map(_.id))
    allocated ++ otherResourceIds.map { id =>
      val request = parseResourceRequest(sparkConf, id)
      ResourceAllocation(id, discoverResource(request).addresses)
    }
  }

  private def assertResourceAllocationMeetsRequest(
      allocation: ResourceAllocation,
      request: ResourceRequest): Unit = {
    require(allocation.id == request.id && allocation.addresses.size >= request.amount,
      s"Resource: ${allocation.id.resourceName}, with addresses: " +
      s"${allocation.addresses.mkString(",")} " +
      s"is less than what the user requested: ${request.amount})")
  }

  private def assertAllResourceAllocationsMeetRequests(
      allocations: Seq[ResourceAllocation],
      requests: Seq[ResourceRequest]): Unit = {
    val allocated = allocations.map(x => x.id -> x).toMap
    requests.foreach(r => assertResourceAllocationMeetsRequest(allocated(r.id), r))
  }

  /**
   * 从输入资源文件中为输入组件获取所有分配的资源信息，并通过发现脚本发现剩余的信息。
   * 它还验证资源分配是否满足每种资源的要求量。
   *
   * @return 从资源名称到资源信息的映射
   */
  def getOrDiscoverAllResources(
      sparkConf: SparkConf,
      componentName: String,
      resourcesFileOpt: Option[String]): Map[String, ResourceInformation] = {
    val requests = parseAllResourceRequests(sparkConf, componentName)
    val allocations = parseAllocatedOrDiscoverResources(sparkConf, componentName, resourcesFileOpt)
    assertAllResourceAllocationsMeetRequests(allocations, requests)
    val resourceInfoMap = allocations.map(a => (a.id.resourceName, a.toResourceInformation)).toMap
    resourceInfoMap
  }

  def logResourceInfo(componentName: String, resources: Map[String, ResourceInformation])
    : Unit = {
    logInfo("==============================================================")
    logInfo(s"Resources for $componentName:\n${resources.mkString("\n")}")
    logInfo("==============================================================")
  }

  // visible for test
  private[spark] def discoverResource(resourceRequest: ResourceRequest): ResourceInformation = {
    val resourceName = resourceRequest.id.resourceName
    val script = resourceRequest.discoveryScript
    val result = if (script.nonEmpty) {
      val scriptFile = new File(script.get)
      // check that script exists and try to execute
      if (scriptFile.exists()) {
        val output = executeAndGetOutput(Seq(script.get), new File("."))
        ResourceInformation.parseJson(output)
      } else {
        throw new SparkException(s"Resource script: $scriptFile to discover $resourceName " +
          "doesn't exist!")
      }
    } else {
      throw new SparkException(s"User is expecting to use resource: $resourceName, but " +
        "didn't specify a discovery script!")
    }
    if (!result.name.equals(resourceName)) {
      throw new SparkException(s"Error running the resource discovery script ${script.get}: " +
        s"script returned resource name ${result.name} and we were expecting $resourceName.")
    }
    result
  }

  // known types of resources
  final val GPU: String = "gpu"
  final val FPGA: String = "fpga"
}
