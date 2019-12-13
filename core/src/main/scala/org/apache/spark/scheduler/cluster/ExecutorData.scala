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

package org.apache.spark.scheduler.cluster

import org.apache.spark.rpc.{RpcAddress, RpcEndpointRef}
import org.apache.spark.scheduler.ExecutorResourceInfo

/**
 * CoarseGrainedSchedulerBackend使用的执行程序的数据分组。
 *
 * @param executorEndpoint 代表该执行程序的RpcEndpointRef
 * @param executorAddress 该执行程序的网络地址
 * @param executorHost 该执行程序正在其上运行的主机名
 * @param freeCores  当前可用于执行程序的内核数
 * @param totalCores 执行程序可用的核心总数
 * @param resourcesInfo 执行器上当前可用资源的信息
 */
private[cluster] class ExecutorData(
    val executorEndpoint: RpcEndpointRef,
    val executorAddress: RpcAddress,
    override val executorHost: String,
    var freeCores: Int,
    override val totalCores: Int,
    override val logUrlMap: Map[String, String],
    override val attributes: Map[String, String],
    override val resourcesInfo: Map[String, ExecutorResourceInfo]
) extends ExecutorInfo(executorHost, totalCores, logUrlMap, attributes, resourcesInfo)
