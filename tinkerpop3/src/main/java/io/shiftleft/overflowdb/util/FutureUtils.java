package io.shiftleft.overflowdb.util;

import io.shiftleft.overflowdb.OdbGraphBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class FutureUtils {
  public static void waitForAll(List<CompletableFuture<Void>> futures) {
    futures.stream().forEach(f -> {
      try {
        f.get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static <T> void parForEach(List<T> list, Consumer<List<? super T>> action, int partitionSize, ExecutorService executor) {
    final int numberOfTasks = list.size() / partitionSize + (list.size() % partitionSize == 0 ? 0 : 1);
    if (numberOfTasks < 2) {
      action.accept(list);
    } else {
      final List<CompletableFuture<Void>> futures = new ArrayList<>(numberOfTasks);
      for (int batchIndex = 0; batchIndex < numberOfTasks; ++batchIndex) {
        int startIndex = batchIndex * partitionSize;
        int endIndex = Math.min(startIndex + partitionSize, list.size());
        final List<T> partition = list.subList(startIndex, endIndex);
        futures.add(CompletableFuture.runAsync(() -> action.accept(partition), executor));
        waitForAll(futures);
      }
    }
  }

  public static void parForEachLong(long[] list, Consumer<long[]> action, int partitionSize, ExecutorService executor) {
    final int numberOfTasks = list.length / partitionSize + (list.length % partitionSize == 0 ? 0 : 1);
    if (numberOfTasks < 2) {
      action.accept(list);
    } else {
      final List<CompletableFuture<Void>> futures = new ArrayList<>(numberOfTasks);
      for (int batchIndex = 0; batchIndex < numberOfTasks; ++batchIndex) {
        int startIndex = batchIndex * partitionSize;
        int endIndex = Math.min(startIndex + partitionSize, list.length);
        long[] partition = Arrays.copyOfRange(list, startIndex, endIndex);
        futures.add(CompletableFuture.runAsync(() -> action.accept(partition), executor));
        waitForAll(futures);
      }
    }
  }
}
