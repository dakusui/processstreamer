package com.github.dakusui.processstreamer.utils;

import com.github.dakusui.processstreamer.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.github.dakusui.processstreamer.utils.ConcurrencyUtils.updateAndNotifyAll;
import static com.github.dakusui.processstreamer.utils.ConcurrencyUtils.waitWhile;
import static java.util.Objects.requireNonNull;

/**
 * = A Stream Utility class
 * This is a stream utility class.
 */
public enum StreamUtils {
  ;
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamUtils.class);

  /**
   * Returns a consumer which writes given string objects to an {@code OutputStream}
   * {@code os} using a {@code Charset} {@code charset}.
   * <p>
   * If {@code null} is given to the consumer returned by this method, the output
   * to {@code os} will be closed and the {@code null} will not be passed to it.
   *
   * @param os      OutputStream to which string objects given to returned consumer written.
   * @param charset A {@code Charset} object that specifies encoding by which
   * @return A closeable strinmg consumer object.
   */
  public static CloseableStringConsumer toCloseableStringConsumer(OutputStream os, Charset charset) {
    try {
      return CloseableStringConsumer.create(os, charset);
    } catch (UnsupportedEncodingException e) {
      throw Exceptions.wrap(e);
    }
  }

  @SafeVarargs
  public static <T> Stream<T> concat(Stream<T>... streams) {
    if (streams.length == 0) {
      return Stream.empty();
    }
    if (streams.length == 1) {
      return streams[0];
    }
    if (streams.length == 2) {
      return Stream.concat(streams[0], streams[1]);
    }
    @SuppressWarnings("unchecked") Stream<T>[] rest = new Stream[streams.length - 1];
    System.arraycopy(streams, 1, rest, 0, rest.length);
    return concat(streams[0], concat(rest));
  }

  public interface CloseableStringConsumer extends Consumer<String>, Closeable {
    @Override
    default void accept(String s) {
      if (s != null)
        this.writeLine(s);
      else
        this.close();
    }

    default void writeLine(String s) {
      this.printStream().println(s);
    }

    @Override
    default void close() {
      //noinspection resource
      printStream().flush();
      printStream().close();
    }

    PrintStream printStream();

    static CloseableStringConsumer create(OutputStream os, Charset charset) throws UnsupportedEncodingException {
      PrintStream ps = new PrintStream(os, true, charset);
      return () -> ps;
    }
  }

  /**
   * Returns a consumer that does nothing.
   *
   * @param <T> Type of values to be consumed by returned object.
   * @return A consumer that doesn't do anything.
   */
  public static <T> Consumer<T> nop() {
    return e -> {
    };
  }

  public static <T> Stream<T> closeOnFinish(Stream<T> in) {
    return onFinish(in, Stream::close).onClose(in::close);
  }

  @SuppressWarnings("unchecked")
  public static <T> Stream<T> onFinish(Stream<T> in, Consumer<Stream<T>> action) {
    Object sentinel = new Object() {
      @Override
      public String toString() {
        return "(sentinel)";
      }
    };
    return Stream.concat(requireNonNull(in), Stream.of(sentinel))
                 .filter(o -> {
                   if (o != sentinel)
                     return true;
                   else {
                     action.accept(in);
                     return false;
                   }
                 })
                 .map(each -> (T) each);
  }

  /**
   * = Merging function
   * Merges given streams possibly block into one keeping orders where elements
   * appear in original streams.
   *
   * [ditaa]
   * ----
   *
   * +-----+
   * |Queue|
   * +-----+
   *
   * ----
   *
   * @param threadPool       A thread pool that gives threads by which data in {@code streams}
   *                         drained to the returned stream.
   * @param threadPoolCloser A consumer that closes {@code threadPool}.
   * @param queueSize        The size of queue
   * @param streams          input streams
   * @param <T>              Type of elements that given streams contain.
   * @return merged stream
   */
  @SafeVarargs
  public static <T> Stream<T> merge(
      ExecutorService threadPool,
      Consumer<ExecutorService> threadPoolCloser,
      int queueSize,
      Stream<T>... streams) {
    if (streams.length == 0)
      return Stream.empty();
    if (streams.length == 1)
      return streams[0];
    BlockingQueue<Object> queue = new ArrayBlockingQueue<>(queueSize);
    Set<Object> sentinels = new HashSet<>();

    AtomicInteger remainingStreams = new AtomicInteger(streams.length);
    int i = 0;
    for (Stream<T> eachStream : streams) {
      Object sentinel = createSentinel(i++);
      sentinels.add(sentinel);
      LOGGER.trace("Submitting task for:{}", sentinel);
      Consumer<Object> action = new Consumer<Object>() {
        boolean started = false;

        @Override
        public void accept(Object elementOrSentinel) {
          LOGGER.trace("{}, is trying to put:{}", sentinel, elementOrSentinel);
          if (!started) {
            LOGGER.trace("task:stream:sentinel={} starting", sentinel);
            synchronized (remainingStreams) {
              updateAndNotifyAll(remainingStreams, AtomicInteger::decrementAndGet);
              started = true;
            }
          }
          putElement(queue, elementOrSentinel);
        }
      };
      threadPool.execute(
          () -> Stream.concat(eachStream, Stream.of(sentinel)).forEach(action)
      );
      LOGGER.trace("Submitted task for:{}", eachStream);
    }
    synchronized (remainingStreams) {
      boolean succeeded = false;
      try {
        waitWhile(remainingStreams, v -> v.get() > 0);
        succeeded = true;
      } finally {
        if (!succeeded)
          LOGGER.error("remainingStreams={}", remainingStreams);
      }
    }
    Supplier<Object> reader = blockingDataReader(queue);
    Set<Object> remainingSentinels = new HashSet<>(sentinels);
    Predicate<Object> isSentinel = sentinels::contains;
    return StreamSupport.stream(new Iterable<T>() {
      final Supplier<Object> readNext = () -> {
        Object nextElementOrSentinel = reader.get();
        if (isSentinel.test(nextElementOrSentinel)) {
          remainingSentinels.remove(nextElementOrSentinel);
          if (remainingSentinels.isEmpty())
            return nextElementOrSentinel;
          else
            return this.readNext.get();
        }
        return nextElementOrSentinel;
      };

      @Override
      public Iterator<T> iterator() {
        return iteratorFinishingOnSentinel(isSentinel, readNext);
      }

    }.spliterator(), false).onClose(() -> threadPoolCloser.accept(threadPool));
  }

  private static <T> Iterator<T> iteratorFinishingOnSentinel(
      Predicate<Object> isSentinel, Supplier<Object> readNext) {
    return new Iterator<T>() {
      /**
       * An object to let this iterator know that the {@code next} field
       * is not valid anymore and it needs to read the next value from the
       * source {@code i}.
       * This is different from  a sentinel.
       */
      private final Object invalid = new Object();
      Object next = invalid;

      @Override
      public boolean hasNext() {
        if (this.next == this.invalid)
          this.next = readNext.get();
        return !isSentinel.test(this.next);
      }

      @SuppressWarnings("unchecked")
      @Override
      public T next() {
        if (this.next == this.invalid)
          this.next = readNext.get();
        if (isSentinel.test(this.next))
          throw new NoSuchElementException();
        try {
          return (T) this.next;
        } finally {
          this.next = this.invalid;
        }
      }
    };
  }

  private static Supplier<Object> blockingDataReader(BlockingQueue<Object> queue) {
    return () -> {
      while (true) {
        try {
          return queue.take();
        } catch (InterruptedException ignored) {
        }
      }
    };
  }

  private static Object createSentinel(int i) {
    return new Object() {
      @Override
      public String toString() {
        return String.format("SENTINEL:%s", i);
      }
    };
  }

  private static void putElement(BlockingQueue<Object> queue, Object e) {
    try {
      queue.put(e);
    } catch (InterruptedException ignored) {
    }
  }

  public static Stream<String> stream(InputStream is, Charset charset) {
    return IoUtils.bufferedReader(is, charset).lines();
  }

  public interface RingBuffer<E> {
    void write(E elem);

    Stream<E> stream();

    static <E> RingBuffer<E> create(int size) {
      return new RingBuffer<E>() {
        int cur = 0;
        final List<E> buffer = new ArrayList<>(size);

        @Override
        public void write(E elem) {
          this.buffer.add(cur++, elem);
          cur %= size;
        }

        @Override
        public synchronized Stream<E> stream() {
          return Stream.concat(
              this.buffer.subList(cur, this.buffer.size()).stream(),
              this.buffer.subList(0, cur).stream());
        }
      };
    }
  }
}
