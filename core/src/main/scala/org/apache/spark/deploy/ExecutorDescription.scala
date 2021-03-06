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

/**
 * 用于将有关执行者的状态从工人在线发送到主人。
 * 此状态足以使主服务器在故障转移期间重建其内部数据结构。
 */
private[deploy] class ExecutorDescription(
    val appId: String,
    val execId: Int,
    val cores: Int,
    val state: ExecutorState.Value)
  extends Serializable {

  override def toString: String =
    "ExecutorState(appId=%s, execId=%d, cores=%d, state=%s)".format(appId, execId, cores, state)
}
