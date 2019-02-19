package com.github.dakusui.processstreamer.ut;

import com.github.dakusui.processstreamer.core.stream.Partitioner;
import com.github.dakusui.processstreamer.utils.ConcurrencyUtils;
import com.github.dakusui.processstreamer.utils.Repeat;
import com.github.dakusui.processstreamer.utils.StreamUtils;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.github.dakusui.processstreamer.utils.ConcurrencyUtils.shutdownThreadPoolAndAwaitTermination;
import static com.github.dakusui.processstreamer.utils.TestUtils.dataStream;
import static com.github.dakusui.crest.Crest.asInteger;
import static com.github.dakusui.crest.Crest.assertThat;

@RunWith(Enclosed.class)
public class PartitioningTest extends SplittingConnectorTest {
  @SuppressWarnings("unchecked")
  private static void executePartitionTest(int numSplits, int numItems, Function<Integer, Function<Integer, List<Stream<String>>>> tee) {
    ExecutorService threadPool = Executors.newFixedThreadPool(numSplits);
    AtomicInteger counter = new AtomicInteger(0);
    List<List<String>> outs = Collections.synchronizedList(new LinkedList<>());
    List<Stream<String>> teedData = tee.apply(numSplits).apply(numItems);
    teedData
        .forEach(
            stream -> {
              final List<String> out = new LinkedList<>();
              outs.add(counter.getAndIncrement(), out);
              threadPool.submit(() -> stream.forEach(out::add));
            }
        );
    shutdownThreadPoolAndAwaitTermination(threadPool);
    List<String> all = new LinkedList<String>() {{
      for (List<String> each : outs)
        addAll(each);
    }};
    assertThat(
        all,
        asInteger("size").equalTo(numItems).$()
    );
  }

  public abstract static class Base {
    @Repeat(times = 1_000)
    @Test(timeout = 10_000)
    public void givenShortStream$thenPartitionInto3$thenStreamedCorrectly() {
      int numSplits = 3;
      int numItems = 10;
      executePartitionTest(numSplits, numItems, partition());
    }

    @Test(timeout = 5_000)
    public void givenShortStream$thenPartitionInto7$thenStreamedCorrectly() {
      int numSplits = 7;
      int numItems = 10;
      executePartitionTest(numSplits, numItems, partition());
    }

    @Test(timeout = 10_000)
    public void givenLongStream$thenPartitionInto3$thenStreamedCorrectly() {
      int numSplits = 3;
      int numItems = 1_000_000;
      executePartitionTest(numSplits, numItems, partition());
    }

    @Test(timeout = 10_000)
    public void givenLongStream$thenPartitionInto1$thenStreamedCorrectly() {
      int numSplits = 1;
      int numItems = 1_000_000;
      executePartitionTest(
          numSplits,
          numItems,
          partition()
      );
    }

    abstract Function<Integer, Function<Integer, List<Stream<String>>>> partition();
  }

  public static class WithStreamUtils extends Base {
    @Override
    Function<Integer, Function<Integer, List<Stream<String>>>> partition() {
      return nS -> nI -> StreamUtils.partition(
          Executors.newFixedThreadPool(nS),
          ConcurrencyUtils::shutdownThreadPoolAndAwaitTermination,
          dataStream("data", nI),
          nS,
          100,
          Object::hashCode
      );
    }
  }

  public static class WithPartitioner extends Base {
    @Override
    Function<Integer, Function<Integer, List<Stream<String>>>> partition() {
      return nS -> nI -> StreamUtils.partition(
          Executors.newFixedThreadPool(nS),
          ConcurrencyUtils::shutdownThreadPoolAndAwaitTermination,
          dataStream("data", nI),
          nS,
          100,
          Object::hashCode
      );
    }
  }

  public static class WithPartitionerConnector extends Base {
    @Override
    Function<Integer, Function<Integer, List<Stream<String>>>> partition() {
      return nS -> nI ->
          new Partitioner.Builder<>(dataStream("data", nI))
              .numQueues(nS)
              .build()
              .partition();
    }
  }

}
