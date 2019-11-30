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

package org.apache.spark.storage

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.util.Utils

/**
 * ::DeveloperApi::
 * TopologyMapper提供给定主机的拓扑信息
 * @param conf SparkConf获取所需的属性（如果需要）
 */
@DeveloperApi
abstract class TopologyMapper(conf: SparkConf) {
  /**
   * 在给定主机名的情况下获取拓扑信息
   *
   * @param hostname Hostname
   * @return 给定主机名的拓扑信息。可以使用“拓扑定界符”来嵌套此拓扑信息。
   *         例如：“ /myrack/myhost”，其中“ /”是拓扑分隔符，“ myrack”是拓扑标识符，“ myhost”是单个主机。
   *         该函数仅返回不包含主机名的拓扑信息。
   *         例如，当选择执行程序块进行块复制时，可以使用此信息来区分与候选执行程序不同的机架中的执行程序。
   *
   *         如果拓扑信息不可用，实现可以选择使用空字符串或“无”。这意味着所有这些执行者都属于同一机架。
   */
  def getTopologyForHost(hostname: String): Option[String]
}

/**
 * 假设所有节点都在同一机架中的拓扑映射器
 */
@DeveloperApi
class DefaultTopologyMapper(conf: SparkConf) extends TopologyMapper(conf) with Logging {
  override def getTopologyForHost(hostname: String): Option[String] = {
    logDebug(s"Got a request for $hostname")
    None
  }
}

/**
 * A simple file based topology mapper. This expects topology information provided as a
 * `java.util.Properties` file. The name of the file is obtained from SparkConf property
 * `spark.storage.replication.topologyFile`. To use this topology mapper, set the
 * `spark.storage.replication.topologyMapper` property to
 * [[org.apache.spark.storage.FileBasedTopologyMapper]]
 * @param conf SparkConf object
 */
@DeveloperApi
class FileBasedTopologyMapper(conf: SparkConf) extends TopologyMapper(conf) with Logging {
  val topologyFile = conf.get(config.STORAGE_REPLICATION_TOPOLOGY_FILE)
  require(topologyFile.isDefined, "Please specify topology file via " +
    "spark.storage.replication.topologyFile for FileBasedTopologyMapper.")
  val topologyMap = Utils.getPropertiesFromFile(topologyFile.get)

  override def getTopologyForHost(hostname: String): Option[String] = {
    val topology = topologyMap.get(hostname)
    if (topology.isDefined) {
      logDebug(s"$hostname -> ${topology.get}")
    } else {
      logWarning(s"$hostname does not have any topology information")
    }
    topology
  }
}

