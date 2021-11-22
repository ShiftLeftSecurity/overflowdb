package io.shiftleft.threadpoolisolation;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;

public class TrivialStructuredConcurrencyContext extends StructuredConcurrencyContext {
    protected volatile ForkJoinPool forkJoinPool = null;
    protected volatile ExecutorService cachedPool = null;


    public TrivialStructuredConcurrencyContext() {
    }

    public ThreadFactory threadFactory() {
        return Executors.defaultThreadFactory();
    }

    public ForkJoinPool forkJoinPool() {
        if (forkJoinPool == null) {
            synchronized (this) {
                if (forkJoinPool == null) {
                    forkJoinPool = makeForkJoinPool();
                }
            }
        }
        return forkJoinPool;
    }

    public ExecutorService cachedPool() {
        if (cachedPool == null) {
            synchronized (this) {
                if (cachedPool == null) {
                    cachedPool = Executors.newCachedThreadPool(threadFactory());
                }
            }
        }
        return cachedPool;
    }


    protected ForkJoinPool makeForkJoinPool() {
        return (ForkJoinPool) Executors.newWorkStealingPool();
    }
}
