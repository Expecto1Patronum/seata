/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.server;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import io.seata.common.XID;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.NetUtil;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.rpc.ShutdownHook;
import io.seata.core.rpc.netty.NettyRemotingServer;
import io.seata.server.coordinator.DefaultCoordinator;
import io.seata.server.env.ContainerHelper;
import io.seata.server.env.PortHelper;
import io.seata.server.metrics.MetricsManager;
import io.seata.server.session.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Server.
 *
 * @author slievrly
 */
public class Server {

    private static final int MIN_SERVER_POOL_SIZE = 50;
    private static final int MAX_SERVER_POOL_SIZE = 500;
    private static final int MAX_TASK_QUEUE_SIZE = 20000;
    private static final int KEEP_ALIVE_TIME = 500;
    private static final ThreadPoolExecutor WORKING_THREADS = new ThreadPoolExecutor(MIN_SERVER_POOL_SIZE,
        MAX_SERVER_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE),
        new NamedThreadFactory("ServerHandlerThread", MAX_SERVER_POOL_SIZE), new ThreadPoolExecutor.CallerRunsPolicy());

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException the io exception
     */
    public static void main(String[] args) throws IOException {
        // get port first, use to logback.xml
        // 从环境变量或运行时参数中获取监听端口，默认端口8091
        int port = PortHelper.getPort(args);
        // 把监听端口设置到SystemProperty中，Logback的LoggerContextListener实现类
        // SystemPropertyLoggerContextListener会把Port写入到Logback的Context中，
        // 在logback.xml文件中会使用Port变量来构建日志文件名称。
        System.setProperty(ConfigurationKeys.SERVER_PORT, Integer.toString(port));

        // create logger
        // 创建Logger
        final Logger logger = LoggerFactory.getLogger(Server.class);
        if (ContainerHelper.isRunningInContainer()) {
            logger.info("The server is running in container.");
        }

        //initialize the parameter parser
        //Note that the parameter parser should always be the first line to execute.
        //Because, here we need to parse the parameters needed for startup.
        // 解析启动以及配置文件的各种配置参数
        ParameterParser parameterParser = new ParameterParser(args);

        //initialize the metrics
        // metrics相关，这里是使用SPI机制获取Registry实例对象
        MetricsManager.get().init();

        // 把从配置文件中读取到的storeMode写入SystemProperty中，方便其他类使用。
        System.setProperty(ConfigurationKeys.STORE_MODE, parameterParser.getStoreMode());

        // 创建NettyRemotingServer实例，NettyRemotingServer是一个基于Netty实现的Rpc框架，
        // 此时并没有初始化，NettyRemotingServer负责与客户端SDK中的TM、RM进行网络通信。
        NettyRemotingServer nettyRemotingServer = new NettyRemotingServer(WORKING_THREADS);
        //server port
        // 设置监听端口
        nettyRemotingServer.setListenPort(parameterParser.getPort());
        // UUIDGenerator初始化，UUIDGenerator基于雪花算法实现，
        // 用于生成全局事务、分支事务的id。
        // 多个Server实例配置不同的ServerNode，保证id的唯一性
        UUIDGenerator.init(parameterParser.getServerNode());
        //log store mode : file, db, redis
        // SessionHolder负责事务日志（状态）的持久化存储，
        // 当前支持file、db、redis三种存储模式，集群部署模式要使用db或redis模式
        // SessionHolder负责Session的持久化，一个Session对象对应一个事务，事务分为两种：全局事务（GlobalSession）和分支事务（BranchSession）。
        // SessionHolder支持file和db两种持久化方式，其中db支持集群模式，推荐使用db。
        SessionHolder.init(parameterParser.getStoreMode());

        // 创建初始化DefaultCoordinator实例，DefaultCoordinator是TC的核心事务逻辑处理类，
        // 底层包含了AT、TCC、SAGA等不同事务类型的逻辑处理。
        DefaultCoordinator coordinator = new DefaultCoordinator(nettyRemotingServer);
        coordinator.init();
        nettyRemotingServer.setHandler(coordinator);
        // register ShutdownHook
        ShutdownHook.getInstance().addDisposable(coordinator);
        ShutdownHook.getInstance().addDisposable(nettyRemotingServer);

        //127.0.0.1 and 0.0.0.0 are not valid here.
        if (NetUtil.isValidIp(parameterParser.getHost(), false)) {
            XID.setIpAddress(parameterParser.getHost());
        } else {
            XID.setIpAddress(NetUtil.getLocalIp());
        }
        XID.setPort(nettyRemotingServer.getListenPort());

        try {
            // 初始化Netty，开始监听端口并阻塞在这里，等待程序关闭
            nettyRemotingServer.init();
        } catch (Throwable e) {
            logger.error("nettyServer init error:{}", e.getMessage(), e);
            System.exit(-1);
        }

        System.exit(0);
    }
}
