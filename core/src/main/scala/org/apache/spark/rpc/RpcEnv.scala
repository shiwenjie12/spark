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

package org.apache.spark.rpc

import java.io.File
import java.nio.channels.ReadableByteChannel

import scala.concurrent.Future

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.rpc.netty.NettyRpcEnvFactory
import org.apache.spark.util.RpcUtils


/**
 * RpcEnv实现必须具有带空构造函数的[[RpcEnvFactory]]实现
 * 以便可以通过反射创建它。
 */
private[spark] object RpcEnv {

  def create(
      name: String,
      host: String,
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager,
      clientMode: Boolean = false): RpcEnv = {
    create(name, host, host, port, conf, securityManager, 0, clientMode)
  }

  def create(
      name: String,
      bindAddress: String,
      advertiseAddress: String,
      port: Int,
      conf: SparkConf,
      securityManager: SecurityManager,
      numUsableCores: Int,
      clientMode: Boolean): RpcEnv = {
    val config = RpcEnvConfig(conf, name, bindAddress, advertiseAddress, port, securityManager,
      numUsableCores, clientMode)
    new NettyRpcEnvFactory().create(config)
  }
}


/**
 * An RPC environment. [[RpcEndpoint]]s need to register itself with a name to [[RpcEnv]] to
 * receives messages. Then [[RpcEnv]] will process messages sent from [[RpcEndpointRef]] or remote
 * nodes, and deliver them to corresponding [[RpcEndpoint]]s. For uncaught exceptions caught by
 * [[RpcEnv]], [[RpcEnv]] will use [[RpcCallContext.sendFailure]] to send exceptions back to the
 * sender, or logging them if no such sender or `NotSerializableException`.
 *
 * [[RpcEnv]] also provides some methods to retrieve [[RpcEndpointRef]]s given name or uri.
 */
private[spark] abstract class RpcEnv(conf: SparkConf) {

  private[spark] val defaultLookupTimeout = RpcUtils.lookupRpcTimeout(conf)

  /**
   * Return RpcEndpointRef of the registered [[RpcEndpoint]]. Will be used to implement
   * [[RpcEndpoint.self]]. Return `null` if the corresponding [[RpcEndpointRef]] does not exist.
   */
  private[rpc] def endpointRef(endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * 返回[[RpcEnv]]正在收听的地址。
   */
  def address: RpcAddress

  /**
   * 用名称注册[[RpcEndpointRef]]并返回其[[RpcEndpointRef]]。 [[RpcEnv]]不保证线程安全。
   */
  def setupEndpoint(name: String, endpoint: RpcEndpoint): RpcEndpointRef

  /**
   * 异步检索由uri表示的[[RpcEndpointRef]]。
   */
  def asyncSetupEndpointRefByURI(uri: String): Future[RpcEndpointRef]

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `uri`. This is a blocking action.
   */
  def setupEndpointRefByURI(uri: String): RpcEndpointRef = {
    defaultLookupTimeout.awaitResult(asyncSetupEndpointRefByURI(uri))
  }

  /**
   * Retrieve the [[RpcEndpointRef]] represented by `address` and `endpointName`.
   * This is a blocking action.
   */
  def setupEndpointRef(address: RpcAddress, endpointName: String): RpcEndpointRef = {
    setupEndpointRefByURI(RpcEndpointAddress(address, endpointName).toString)
  }

  /**
   * Stop [[RpcEndpoint]] specified by `endpoint`.
   */
  def stop(endpoint: RpcEndpointRef): Unit

  /**
   * Shutdown this [[RpcEnv]] asynchronously. If need to make sure [[RpcEnv]] exits successfully,
   * call [[awaitTermination()]] straight after [[shutdown()]].
   */
  def shutdown(): Unit

  /**
   * Wait until [[RpcEnv]] exits.
   *
   * TODO do we need a timeout parameter?
   */
  def awaitTermination(): Unit

  /**
   * [[RpcEndpointRef]] cannot be deserialized without [[RpcEnv]]. So when deserializing any object
   * that contains [[RpcEndpointRef]]s, the deserialization codes should be wrapped by this method.
   */
  def deserialize[T](deserializationAction: () => T): T

  /**
   * 返回用于提供文件的文件服务器的实例。 如果RpcEnv不在服务器模式下运行，则该值为“ null”。
   */
  def fileServer: RpcEnvFileServer

  /**
   * Open a channel to download a file from the given URI. If the URIs returned by the
   * RpcEnvFileServer use the "spark" scheme, this method will be called by the Utils class to
   * retrieve the files.
   *
   * @param uri URI with location of the file.
   */
  def openChannel(uri: String): ReadableByteChannel
}

/**
 * RpcEnv使用的服务器，用于将文件服务器提供给应用程序所拥有的其他进程。
 *
 * 文件服务器可以返回由公共库处理的URI（例如“ http”或“ hdfs”），也可以返回将由`RpcEnv#fetchFile`处理的“ spark” URI。
 *  The file server can return URIs handled by common libraries (such as "http" or "hdfs"), or
 * it can return "spark" URIs which will be handled by `RpcEnv#fetchFile`.
 */
private[spark] trait RpcEnvFileServer {

  /**
   * 添加此RpcEnv服务的文件。当文件存储在驱动程序的本地文件系统上时，用于将文件从驱动程序提供给执行程序。
   *
   * @param file Local file to serve.
   * @return A URI for the location of the file.
   */
  def addFile(file: File): String

  /**
   * Adds a jar to be served by this RpcEnv. Similar to `addFile` but for jars added using
   * `SparkContext.addJar`.
   *
   * @param file Local file to serve.
   * @return A URI for the location of the file.
   */
  def addJar(file: File): String

  /**
   * Adds a local directory to be served via this file server.
   *
   * @param baseUri Leading URI path (files can be retrieved by appending their relative
   *                path to this base URI). This cannot be "files" nor "jars".
   * @param path Path to the local directory.
   * @return URI for the root of the directory in the file server.
   */
  def addDirectory(baseUri: String, path: File): String

  /** Validates and normalizes the base URI for directories. */
  protected def validateDirectoryUri(baseUri: String): String = {
    val fixedBaseUri = "/" + baseUri.stripPrefix("/").stripSuffix("/")
    require(fixedBaseUri != "/files" && fixedBaseUri != "/jars",
      "Directory URI cannot be /files nor /jars.")
    fixedBaseUri
  }

}

private[spark] case class RpcEnvConfig(
    conf: SparkConf,
    name: String,
    bindAddress: String,
    advertiseAddress: String,
    port: Int,
    securityManager: SecurityManager,
    numUsableCores: Int,
    clientMode: Boolean)
