package overflowdb.util;

import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.ThreadFactory;

public class NamedThreadFactory implements ThreadFactory {
  private final String threadName;
  private final Map<String, String> mdc = MDC.getCopyOfContextMap();

  public NamedThreadFactory(String threadName) {
    this.threadName = threadName;
  }

  public Thread newThread(Runnable r) {
    return new Thread(() -> {MDC.setContextMap(mdc); r.run();}, threadName);
  }
}
