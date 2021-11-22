package io.shiftleft.threadpoolisolation;

import java.util.concurrent.*;

abstract public class StructuredConcurrencyContext implements AutoCloseable {
    protected static final InheritableThreadLocal<StructuredConcurrencyContext> currentContext = new InheritableThreadLocal<>();
    private static final Object monitor = new Object();
    public static volatile boolean returnDefaultOnMissing = true;
    private static volatile ExecutorService globalExecutor = null;

    public static StructuredConcurrencyContext getCurrent() {
        return currentContext.get();
    }

    public static ForkJoinPool getForkJoinPool() {
        StructuredConcurrencyContext ctx = getCurrent();
        if (ctx != null) return ctx.forkJoinPool();
        else if (ctx == null && !returnDefaultOnMissing) return null;
        else {
            //we are in the error-case where we don't have a context, but we're supposed to return something.
            //standard procedure is: ForkJoin from within ForkJoin uses same pool, otherwise global common pool.
            Thread t = Thread.currentThread();
            if (t instanceof ForkJoinWorkerThread)
                return ((ForkJoinWorkerThread) t).getPool();
            else return ForkJoinPool.commonPool();
        }
    }

    public static ExecutorService getCachedPool() {
        StructuredConcurrencyContext ctx = getCurrent();
        if (ctx != null) return ctx.cachedPool();
        else if (ctx == null && !returnDefaultOnMissing) return null;
        else {
            //we are in the error-case where we don't have a context, but we're supposed to return something.
            //there is no global common pool of this type, so we create one.
            if (globalExecutor != null)
                return globalExecutor;
            else synchronized (monitor) {
                if (globalExecutor == null)
                    globalExecutor = Executors.newCachedThreadPool();
                return globalExecutor;
            }
        }
    }

    public static ThreadFactory getThreadFactory() {
        StructuredConcurrencyContext ctx = getCurrent();
        if (ctx != null) return ctx.threadFactory();
        else if (ctx == null && !returnDefaultOnMissing) return null;
        else return Executors.defaultThreadFactory();
    }

    public void enterOnce(Runnable r){
        try {
            enter(r);
        } finally {
            close();
        }
    }

    public void enter(Runnable r) {
        Thread t = Thread.currentThread();
        if (t instanceof ForkJoinWorkerThread)
            throw new RuntimeException("Cannot enter StructuredConcurrencyContext from within ForkJoinWorkerThread");
        StructuredConcurrencyContext oldCtx = getCurrent();
        Object unrelatedContexts = entryHook();
        try {
            currentContext.set(this);
            r.run();
        } finally {
            currentContext.set(oldCtx);
            exitHook(unrelatedContexts);
        }
    }

    public void shutdownPools() {
        this.forkJoinPool().shutdown();
        this.cachedPool().shutdown();
    }

    public void close() {
        shutdownPools();
    }

    protected Object entryHook() {
        return null;
    }

    protected void exitHook(Object obj) {
    }

    public abstract ForkJoinPool forkJoinPool();

    public abstract ExecutorService cachedPool();

    public abstract ThreadFactory threadFactory();
}
