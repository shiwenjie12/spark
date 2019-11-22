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

package org.apache.spark.deploy

import java.net.URI

import org.apache.spark.resource.ResourceRequirement

/*
  应用描述信息
 */
private[spark] case class ApplicationDescription(
    name: String,
    maxCores: Option[Int],
    memoryPerExecutorMB: Int,
    command: Command,
    appUiUrl: String,
    eventLogDir: Option[URI] = None,
    // 编写事件日志时使用的压缩编解码器的简称（如果有的话）（例如lzf）
    eventLogCodec: Option[String] = None,
    coresPerExecutor: Option[Int] = None,
    // 此应用程序要开始使用的执行程序的数量，仅在启用了动态分配的情况下使用
    initialExecutorLimit: Option[Int] = None,
    user: String = System.getProperty("user.name", "<unknown>"),
    resourceReqsPerExecutor: Seq[ResourceRequirement] = Seq.empty) {

  override def toString: String = "ApplicationDescription(" + name + ")"
}
