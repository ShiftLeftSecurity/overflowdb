package io.shiftleft.threadpoolisolation;

import org.slf4j.MDC;

import java.util.Map;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;

public class MDCStructuredConcurrencyContext extends TrivialStructuredConcurrencyContext {
    Map<String, String> contextMap;

    public MDCStructuredConcurrencyContext(Map<String, String> contextMap) {
        this.contextMap = contextMap;
    }

    public ThreadFactory threadFactory() {
        return new MDCThreadFactory();
    }

    protected Object entryHook() {
        Object oldMDC = MDC.getCopyOfContextMap();
        MDC.setContextMap(contextMap);
        return oldMDC;
    }

    protected void exitHook(Object obj) {
        if (obj != null) {
            MDC.setContextMap((Map<String, String>) obj);
        }
    }

    protected ForkJoinPool makeForkJoinPool() {
        return new ForkJoinPool
                (Runtime.getRuntime().availableProcessors(),
                        new MDCForkJoinThreadFactory(),
                        null, true);
    }


    class MDCThreadFactory implements ThreadFactory {
        public Thread newThread(Runnable r) {
            return new Thread(() -> {
                entryHook();
                r.run();
            });
        }
    }

    class MDCForkJoinThread extends ForkJoinWorkerThread {
        public MDCForkJoinThread(ForkJoinPool pool) {
            super(pool);
        }

        @Override
        public void run() {
            entryHook();
            super.run();
        }
    }

    class MDCForkJoinThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {
        public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
            return new MDCForkJoinThread(pool);
        }
    }

}
