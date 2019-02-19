package com.github.dakusui.processstreamer.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public enum ConcurrencyUtils {
  ;
  private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrencyUtils.class);

  public static <T> void updateAndNotifyAll(T monitor, Consumer<T> update) {
    LOGGER.trace("monitor={} updating", monitor);
    update.accept(monitor);
    LOGGER.trace("monitor updated to={}", monitor);
    monitor.notifyAll();
  }

  public static <T> void waitWhile(T monitor, Predicate<T> cond) {
    while (cond.test(monitor)) {
      LOGGER.trace("waiting on monitor={}", monitor);
      try {
        monitor.wait();
      } catch (InterruptedException ignored) {
      }
    }
  }

  public static void shutdownThreadPoolAndAwaitTermination(ExecutorService threadPool) {
    threadPool.shutdown();
    while (!threadPool.isTerminated()) {
      try {
        threadPool.awaitTermination(1, MILLISECONDS);
      } catch (InterruptedException ignored) {
      }
    }
  }
}
