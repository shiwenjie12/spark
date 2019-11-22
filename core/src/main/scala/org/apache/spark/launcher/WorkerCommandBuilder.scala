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

package org.apache.spark.launcher

import java.io.File
import java.util.{HashMap => JHashMap, List => JList, Map => JMap}

import scala.collection.JavaConverters._

import org.apache.spark.deploy.Command

/**
  * 此类由CommandUtils使用。它在SparkLauncher中使用了一些程序包私有的API，
  * 并且由于Java不具有类似于`private [spark]`的功能，并且我们不希望该类是公共的，
  * 因此需要与其余程序一起生活在同一个程序包中库。
 */
private[spark] class WorkerCommandBuilder(sparkHome: String, memoryMb: Int, command: Command)
    extends AbstractCommandBuilder {

  childEnv.putAll(command.environment.asJava)
  childEnv.put(CommandBuilderUtils.ENV_SPARK_HOME, sparkHome)

  override def buildCommand(env: JMap[String, String]): JList[String] = {
    val cmd = buildJavaCommand(command.classPathEntries.mkString(File.pathSeparator))
    cmd.add(s"-Xmx${memoryMb}M")
    command.javaOpts.foreach(cmd.add)
    cmd
  }

  def buildCommand(): JList[String] = buildCommand(new JHashMap[String, String]())

}
