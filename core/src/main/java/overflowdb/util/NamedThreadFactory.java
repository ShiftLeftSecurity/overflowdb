package overflowdb.util;

import org.slf4j.MDC;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadFactory;

public class NamedThreadFactory implements ThreadFactory {
  private final String threadName;
  private final Map<String, String> mdc;

  public NamedThreadFactory(String threadName) {

    this.threadName = threadName;
    Map<String, String> mdcTmp = MDC.getCopyOfContextMap();
    //logback chokes on null-maps
    this.mdc = mdcTmp != null ? mdcTmp : new HashMap<>();
  }

  public Thread newThread(Runnable r) {
    return new Thread(() -> {MDC.setContextMap(mdc); r.run();}, threadName);
  }
}
