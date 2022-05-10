package com.github.dakusui.processstreamer.ut;

import com.github.dakusui.processstreamer.core.stream.Tee;
import com.github.dakusui.processstreamer.utils.ConcurrencyUtils;
import com.github.dakusui.processstreamer.utils.Repeat;
import com.github.dakusui.processstreamer.utils.StreamUtils;
import com.github.dakusui.crest.core.Matcher;
import org.junit.Ignore;
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
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.github.dakusui.processstreamer.utils.ConcurrencyUtils.shutdownThreadPoolAndAwaitTermination;
import static com.github.dakusui.processstreamer.utils.TestUtils.dataStream;
import static com.github.dakusui.crest.Crest.*;
import static java.util.stream.Collectors.toList;

@RunWith(Enclosed.class)
public class TeeTest extends SplittingConnectorTest {

  @SuppressWarnings("unchecked")
  private static void executeTeeTest(int numSplits, int numItems, Function<Integer, Function<Integer, List<Stream<String>>>> tee) {
    ExecutorService threadPool = Executors.newFixedThreadPool(numSplits);
    AtomicInteger counter = new AtomicInteger(0);
    List<List<String>> outs = Collections.synchronizedList(new LinkedList<>());
    List<Stream<String>> teedData = tee.apply(numSplits).apply(numItems);
    teedData
        .forEach(
            stream -> {
              final List<String> out = new LinkedList<>();
              outs.add(counter.getAndIncrement(), out);

              threadPool.submit(() -> {
                stream.forEach(out::add);
              });
            }
        );
    shutdownThreadPoolAndAwaitTermination(threadPool);
    @SuppressWarnings("unchecked") List<Matcher<List<List<String>>>> matchers = List.class.cast(IntStream.range(0, numSplits)
        .mapToObj(
            i -> asInteger(
                call("get", i)
                    .andThen("size").$()).equalTo(numItems).$())
        .collect(toList()));
    assertThat(
        outs,
        allOf(
            matchers.toArray(new Matcher[0])
        )
    );
  }

  public abstract static class Base {
    @Ignore
    @Repeat(times = 1_000)
    @Test(timeout = 10_000)
    public void givenShortStream$thenTeeInto3$thenStreamedCorrectly() {
      int numSplits = 3;
      int numItems = 10;
      executeTeeTest(numSplits, numItems, tee());
    }

    @Test(timeout = 5_000)
    public void givenShortStream$thenTeeInto7$thenStreamedCorrectly() {
      int numSplits = 7;
      int numItems = 10;
      executeTeeTest(numSplits, numItems, tee());
    }

    @Ignore
    @Test(timeout = 10_000)
    public void givenLongStream$thenTeeInto3$thenStreamedCorrectly() {
      int numSplits = 3;
      int numItems = 1_000_000;
      executeTeeTest(numSplits, numItems, tee());
    }

    @Test(timeout = 10_000)
    public void givenLongStream$thenTeeInto1$thenStreamedCorrectly() {
      int numSplits = 1;
      int numItems = 1_000_000;
      executeTeeTest(
          numSplits,
          numItems,
          tee()
      );
    }

    abstract Function<Integer, Function<Integer, List<Stream<String>>>> tee();
  }

  public static class WithStreamUtils extends Base {
    @Override
    Function<Integer, Function<Integer, List<Stream<String>>>> tee() {
      return nS -> nI -> StreamUtils.tee(
          Executors.newFixedThreadPool(nS),
          ConcurrencyUtils::shutdownThreadPoolAndAwaitTermination,
          dataStream("data", nI),
          nS,
          100
      );
    }
  }

  public static class WithTeeConnector extends Base {
    @Override
    Function<Integer, Function<Integer, List<Stream<String>>>> tee() {
      return nS -> nI ->
          new Tee.Builder<>(dataStream("data", nI))
              .numQueues(nS)
              .build()
              .tee();
    }
  }
}
