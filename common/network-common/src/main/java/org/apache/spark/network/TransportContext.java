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

package org.apache.spark.network;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;

import com.codahale.metrics.Counter;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.network.client.TransportClient;
import org.apache.spark.network.client.TransportClientBootstrap;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.client.TransportResponseHandler;
import org.apache.spark.network.protocol.MessageDecoder;
import org.apache.spark.network.protocol.MessageEncoder;
import org.apache.spark.network.server.ChunkFetchRequestHandler;
import org.apache.spark.network.server.RpcHandler;
import org.apache.spark.network.server.TransportChannelHandler;
import org.apache.spark.network.server.TransportRequestHandler;
import org.apache.spark.network.server.TransportServer;
import org.apache.spark.network.server.TransportServerBootstrap;
import org.apache.spark.network.util.IOMode;
import org.apache.spark.network.util.NettyUtils;
import org.apache.spark.network.util.TransportConf;
import org.apache.spark.network.util.TransportFrameDecoder;

/**
 * 包含用于创建{@link TransportServer}，{@link TransportClientFactory}和
 * 使用{@link org.apache.spark.network.server.TransportChannelHandler}设置Netty Channel管道。
 *
 * There are two communication protocols that the TransportClient provides, control-plane RPCs and
 * data-plane "chunk fetching". The handling of the RPCs is performed outside of the scope of the
 * TransportContext (i.e., by a user-provided handler), and it is responsible for setting up streams
 * which can be streamed through the data plane in chunks using zero-copy IO.
 *
 * The TransportServer and TransportClientFactory both create a TransportChannelHandler for each
 * channel. As each TransportChannelHandler contains a TransportClient, this enables server
 * processes to send messages back to the client on an existing channel.
 */
public class TransportContext implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(TransportContext.class);

  private final TransportConf conf;
  private final RpcHandler rpcHandler;
  private final boolean closeIdleConnections;
  // 与随机播放服务的已注册连接数
  private Counter registeredConnections = new Counter();

  /**
   * Force to create MessageEncoder and MessageDecoder so that we can make sure they will be created
   * before switching the current context class loader to ExecutorClassLoader.
   *
   * Netty's MessageToMessageEncoder uses Javassist to generate a matcher class and the
   * implementation calls "Class.forName" to check if this calls is already generated. If the
   * following two objects are created in "ExecutorClassLoader.findClass", it will cause
   * "ClassCircularityError". This is because loading this Netty generated class will call
   * "ExecutorClassLoader.findClass" to search this class, and "ExecutorClassLoader" will try to use
   * RPC to load it and cause to load the non-exist matcher class again. JVM will report
   * `ClassCircularityError` to prevent such infinite recursion. (See SPARK-17714)
   */
  private static final MessageEncoder ENCODER = MessageEncoder.INSTANCE;
  private static final MessageDecoder DECODER = MessageDecoder.INSTANCE;

  // 用于处理ChunkFetchRequest的单独线程池。这有助于启用限制最大数量的TransportServer工作线程，
  // 这些线程在通过基础通道将ChunkFetchRequest消息的响应写回到客户端时被阻止。
  private final EventLoopGroup chunkFetchWorkers;

  public TransportContext(TransportConf conf, RpcHandler rpcHandler) {
    this(conf, rpcHandler, false, false);
  }

  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections) {
    this(conf, rpcHandler, closeIdleConnections, false);
  }

  /**
   * 为基础客户端和服务器启用TransportContext初始化。
   *
   * @param conf TransportConf
   * @param rpcHandler RpcHandler responsible for handling requests and responses.
   * @param closeIdleConnections Close idle connections if it is set to true.
   * @param isClientOnly This config indicates the TransportContext is only used by a client.
   *                     This config is more important when external shuffle is enabled.
   *                     It stops creating extra event loop and subsequent thread pool
   *                     for shuffle clients to handle chunked fetch requests.
   */
  public TransportContext(
      TransportConf conf,
      RpcHandler rpcHandler,
      boolean closeIdleConnections,
      boolean isClientOnly) {
    this.conf = conf;
    this.rpcHandler = rpcHandler;
    this.closeIdleConnections = closeIdleConnections;

    if (conf.getModuleName() != null &&
        conf.getModuleName().equalsIgnoreCase("shuffle") &&
        !isClientOnly) {
      chunkFetchWorkers = NettyUtils.createEventLoop(
          IOMode.valueOf(conf.ioMode()),
          conf.chunkFetchHandlerThreads(),
          "shuffle-chunk-fetch-handler");
    } else {
      chunkFetchWorkers = null;
    }
  }

  /**
   * Initializes a ClientFactory which runs the given TransportClientBootstraps prior to returning
   * a new Client. Bootstraps will be executed synchronously, and must run successfully in order
   * to create a Client.
   */
  public TransportClientFactory createClientFactory(List<TransportClientBootstrap> bootstraps) {
    return new TransportClientFactory(this, bootstraps);
  }

  public TransportClientFactory createClientFactory() {
    return createClientFactory(new ArrayList<>());
  }

  /** 创建一个将尝试绑定到特定端口的服务器。 */
  public TransportServer createServer(int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, null, port, rpcHandler, bootstraps);
  }

  /** 创建将尝试绑定到特定主机和端口的服务器。 */
  public TransportServer createServer(
      String host, int port, List<TransportServerBootstrap> bootstraps) {
    return new TransportServer(this, host, port, rpcHandler, bootstraps);
  }

  /** Creates a new server, binding to any available ephemeral port. */
  public TransportServer createServer(List<TransportServerBootstrap> bootstraps) {
    return createServer(0, bootstraps);
  }

  public TransportServer createServer() {
    return createServer(0, new ArrayList<>());
  }

  public TransportChannelHandler initializePipeline(SocketChannel channel) {
    return initializePipeline(channel, rpcHandler);
  }

  /**
   * 初始化客户端或服务器Netty Channel管道，该管道对消息进行编码/解码，
   * 并具有{@link org.apache.spark.network.server.TransportChannelHandler}来处理请求或响应消息。
   *
   * @param channel 要初始化的通道。
   * @param channelRpcHandler 用于通道的RPC处理程序。
   *
   * @return Returns the created TransportChannelHandler, which includes a TransportClient that can
   * be used to communicate on this channel. The TransportClient is directly associated with a
   * ChannelHandler to ensure all users of the same channel get the same TransportClient object.
   */
  public TransportChannelHandler initializePipeline(
      SocketChannel channel,
      RpcHandler channelRpcHandler) {
    try {
      TransportChannelHandler channelHandler = createChannelHandler(channel, channelRpcHandler);
      ChunkFetchRequestHandler chunkFetchHandler =
        createChunkFetchHandler(channelHandler, channelRpcHandler);
      ChannelPipeline pipeline = channel.pipeline()
        .addLast("encoder", ENCODER)
        .addLast(TransportFrameDecoder.HANDLER_NAME, NettyUtils.createFrameDecoder())
        .addLast("decoder", DECODER)
        .addLast("idleStateHandler",
          new IdleStateHandler(0, 0, conf.connectionTimeoutMs() / 1000))
        // NOTE: Chunks are currently guaranteed to be returned in the order of request, but this
        // would require more logic to guarantee if this were not part of the same event loop.
        .addLast("handler", channelHandler);
      // 使用单独的EventLoopGroup处理Shuffle RPC的ChunkFetchRequest消息。
      if (chunkFetchWorkers != null) {
        pipeline.addLast(chunkFetchWorkers, "chunkFetchHandler", chunkFetchHandler);
      }
      return channelHandler;
    } catch (RuntimeException e) {
      logger.error("Error while initializing Netty pipeline", e);
      throw e;
    }
  }

  /**
   * 创建用于处理RequestMessages和ResponseMessages的服务器和客户端处理程序。
   * 尽管某些属性（例如remoteAddress()）可能尚不可用，但是该通道应该已成功创建。
   */
  private TransportChannelHandler createChannelHandler(Channel channel, RpcHandler rpcHandler) {
    TransportResponseHandler responseHandler = new TransportResponseHandler(channel);
    TransportClient client = new TransportClient(channel, responseHandler);
    TransportRequestHandler requestHandler = new TransportRequestHandler(channel, client,
      rpcHandler, conf.maxChunksBeingTransferred());
    return new TransportChannelHandler(client, responseHandler, requestHandler,
      conf.connectionTimeoutMs(), closeIdleConnections, this);
  }

  /**
   * 为ChunkFetchRequest消息创建专用的ChannelHandler。
   */
  private ChunkFetchRequestHandler createChunkFetchHandler(TransportChannelHandler channelHandler,
      RpcHandler rpcHandler) {
    return new ChunkFetchRequestHandler(channelHandler.getClient(),
      rpcHandler.getStreamManager(), conf.maxChunksBeingTransferred());
  }

  public TransportConf getConf() { return conf; }

  public Counter getRegisteredConnections() {
    return registeredConnections;
  }

  public void close() {
    if (chunkFetchWorkers != null) {
      chunkFetchWorkers.shutdownGracefully();
    }
  }
}
