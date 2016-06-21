package ru.semiot.platform.smart.client;

import io.netty.channel.nio.NioEventLoopGroup;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 *
 * @author Daniil Garayzuev <garayzuev@gmail.com>
 */
public class ExtNettyWampClientConnectorProvider extends ws.wamp.jawampa.transport.netty.NettyWampClientConnectorProvider{
  private static volatile int count = 0;
  @Override
    public ScheduledExecutorService createScheduler() {
        NioEventLoopGroup scheduler = new NioEventLoopGroup(20, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              synchronized((Integer)count){
                Thread t = new Thread(r, "WampClientEventLoop-"+count++);
                t.setDaemon(true);
                return t;
              }
            }
        });
        return scheduler;
    }
}
