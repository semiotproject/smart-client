package ru.semiot.platform.smartclient.wamp;

import io.netty.channel.nio.NioEventLoopGroup;
import ws.wamp.jawampa.transport.netty.NettyWampClientConnectorProvider;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * @author Daniil Garayzuev <garayzuev@gmail.com>
 */
public class ExtNettyWampClientConnectorProvider extends NettyWampClientConnectorProvider {
  private static volatile int count = 0;

  @Override
  public ScheduledExecutorService createScheduler() {
    NioEventLoopGroup scheduler = new NioEventLoopGroup(2, r -> {
      synchronized ((Integer) count) {
        Thread t = new Thread(r, "WampClientEventLoop-" + count++);
        t.setDaemon(true);
        return t;
      }
    });
    return scheduler;
  }
}
