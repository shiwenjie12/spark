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

package org.apache.spark.launcher;

import java.io.Serializable;

/**
 * 启动器通信协议的消息定义。这些消息必须保持向后兼容，以便启动程序可以与支持该协议的较旧版本的Spark对话。
 */
final class LauncherProtocol {

  /** 服务器端口所在的环境变量。 */
  static final String ENV_LAUNCHER_PORT = "_SPARK_LAUNCHER_PORT";

  /** 环境变量，用于存储连接回服务器的机密。 */
  static final String ENV_LAUNCHER_SECRET = "_SPARK_LAUNCHER_SECRET";

  /** Spark conf密钥，用于传播服务器端口以进行进程内启动。 */
  static final String CONF_LAUNCHER_PORT = "spark.launcher.port";

  /** Spark conf密钥，用于传播进程内启动的应用程序密钥。 */
  static final String CONF_LAUNCHER_SECRET = "spark.launcher.secret";

  static class Message implements Serializable {

  }

  /**
   * Hello message, sent from client to server.
   */
  static class Hello extends Message {

    final String secret;
    final String sparkVersion;

    Hello(String secret, String version) {
      this.secret = secret;
      this.sparkVersion = version;
    }

  }

  /**
   * SetAppId message, sent from client to server.
   */
  static class SetAppId extends Message {

    final String appId;

    SetAppId(String appId) {
      this.appId = appId;
    }

  }

  /**
   * SetState message, sent from client to server.
   */
  static class SetState extends Message {

    final SparkAppHandle.State state;

    SetState(SparkAppHandle.State state) {
      this.state = state;
    }

  }

  /**
   * Stop message, send from server to client to stop the application.
   */
  static class Stop extends Message {

  }

}
