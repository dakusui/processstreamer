package com.github.dakusui.processstreamer.ut;

import com.github.dakusui.processstreamer.core.process.ContextualCommandInvoker;
import com.github.dakusui.processstreamer.core.process.ProcessStreamer;
import com.github.dakusui.processstreamer.ututils.TestUtils;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.dakusui.processstreamer.core.process.ProcessStreamer.*;
import static com.github.dakusui.processstreamer.ututils.TestUtils.dataStream;
import static com.github.dakusui.processstreamer.ututils.TestUtils.lastElementToBe;
import static com.github.valid8j.classic.TestAssertions.assertThat;
import static com.github.valid8j.fluent.Expectations.*;
import static com.github.valid8j.pcond.forms.Predicates.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeout;

public class ProcessStreamerTest extends TestUtils.TestBase {
  @Nested
  class BuilderTest extends TestUtils.TestBase {
    @Test
    public void givenEnvVarHELLO_world_$whenEchoEnvVarHELLO$then_world_isPrinted() {
      ProcessStreamer ps = source().command("echo $HELLO")
                                   .env("HELLO", "world")
                                   .build();

      assertStatement(value(ps.stream().collect(Collectors.joining())).toBe().equalTo("world"));
    }

    @Test
    public void givenCwd$whenEchoEnvVarHELLO$then_world_isPrinted() {
      File dir = new File(System.getProperty("user.dir")).getParentFile();
      ProcessStreamer ps = source().command("pwd")
                                   .cwd(dir)
                                   .configureStdout(false, false, true)
                                   .build();

      assertStatement(value(ps.stream().collect(Collectors.joining())).toBe().equalTo(dir.getAbsolutePath()));
    }

    @Test
    public void givenCommandNotFound$whenStreamClosed$thenExceptionThrown() {
      assertThrows(ProcessStreamer.Failure.class, () -> {
        String commandNotFound = "__command_not_found__";
        ProcessStreamer ps = source()
            .command(commandNotFound)
            .checker(ProcessStreamer.Checker.createDefault())
            .build();

        try (Stream<String> s = ps.stream()) {
          s.forEach(System.out::println);
        } catch (ProcessStreamer.Failure e) {
          assertStatement(value(e.getMessage()).toBe().containing(commandNotFound));
          throw e;
        }
      });
    }

    @Nested
    public class LifeCycleMethodsTest extends TestUtils.TestBase {
      @Test
      public void givenSleepOneSecond$whenDestroy$thenEventuallyDead() {
        assertTimeout(Duration.ofMillis(1_000), () -> {
          ProcessStreamer ps = source().command("sleep 1").build();
          ps.destroy();
          while (ps.isAlive()) {
            try {
              TimeUnit.MICROSECONDS.sleep(1);
            } catch (InterruptedException ignored) {
            }
          }
          assertStatement(value(ps.isAlive()).toBe().falseValue());
        });
      }
    }

    @Nested
    public class SinkTest extends TestUtils.TestBase {
      @Test
      public void testSink() {
        assertTimeout(Duration.ofMillis(3_000), () -> {
          ProcessStreamer ps = sink(dataStream("data-", 10_000))
              .command(String.format("cat -n > %s", File.createTempFile("processstreamer-", "tmp")))
              .build();
          ps.stream().forEach(System.out::println);
        });
      }

      @Test
      public void testSinkBigger() {
        assertTimeout(Duration.ofMillis(3_000), () -> {
          ProcessStreamer ps = sink(dataStream("data-", 100_000))
              .command(String.format("cat -n > %s", File.createTempFile("processstreamer-", "tmp")))
              .build();
          ps.stream().forEach(System.out::println);
        });
      }
    }

    @Nested
    class SourceTest extends TestUtils.TestBase {
      @Test
      public void givenEmptyStream$whenCommandThatWritesOneMilliOnLongLines$thenEventuallyFinishes() {
        assertTimeout(Duration.ofMillis(20_000),
                      () -> source().command("for i in $(seq 1 1000); do seq 1 10000 | paste -s -d ' ' - ; done")
                                    .build()
                                    .stream()
                                    .forEach(System.out::println));
      }

      @Test
      public void givenStreamImmediatelyCloses$whenCommandWritingTenThousandLines$thenEventuallyFinishes() {
        assertTimeout(Duration.ofSeconds(10), () -> source().command("seq 1 100000")
                                                            .build()
                                                            .stream().forEach(System.out::println));
      }

      @Test
      public void givenStreamImmediatelyCloses$whenCommandWritingOneThousandLines$thenEventuallyFinishes() {
        assertTimeout(Duration.ofMillis(1_0000), () -> source(ContextualCommandInvoker.local()).command("seq 1 1000")
                                                                                               .build()
                                                                                               .stream()
                                                                                               .forEach(System.out::println));
      }

      @Test
      public void givenCommandResultingInError$whenExecuted$thenOutputIsCorrect() {
        class Result {
          private final ProcessStreamer ps = source()
              .command("echo hello world && _Echo hello!")
              .configureStderr(true, true, true)
              .checker(ProcessStreamer.Checker.createCheckerForExitCode(127))
              .build();
          private int exitCode;
          private final List<String> out = new LinkedList<>();

          /*
           * This method is reflectively called.
           */
          @SuppressWarnings("unused")
          public int exitCode() {
            return this.exitCode;
          }

          /*
           * This method is reflectively called.
           */
          @SuppressWarnings("unused")
          public List<String> out() {
            return this.out;
          }

          /*
           * This method is reflectively called.
           */
          @SuppressWarnings("unused")
          public ProcessStreamer processStreamer() {
            return this.ps;
          }
        }
        Result result = new Result();
        result.ps.stream().peek(System.out::println).forEach(result.out::add);
        result.exitCode = result.ps.exitValue();

        System.out.println(result.ps.getPid() + "=" + result.exitCode);
        result.out.forEach(System.out::println);

        assertAll(
            value(result).invoke("out")
                         .asListOf(String.class)
                         .toObject(v -> String.join(String.format("%n"), v))
                         .asString()
                         .toBe()
                         .containingSubstrings("_Echo",
                                               "not found",
                                               "hello world"),
            value(result).invoke("exitCode").toBe().equalTo(127),
            value(result).invoke("processStreamer")
                         .invoke("toString")
                         .asString()
                         .toBe()
                         .containing("hello world")
                         .containing("not found"));
      }

      @Test
      public void givenEchos$whenStream$thenOutputIsCorrectAndInOrder() throws InterruptedException {
        assertTimeout(Duration.ofMillis(1_000), () -> {
        });
        assertStatement(
            value(runProcessStreamer(
                () -> sink(Stream.of("a", "b", "c"))
                    .command("echo hello world && echo !")
                    .build()))
                .toBe()
                .containingElementsInOrder("hello world", "!"));
      }

      @Test()
      public void givenUnknownCommand$whenStream$thenFailureThrown() {
        assertTimeout(Duration.ofMillis(1_000), () -> {
          assertThrows(ProcessStreamer.Failure.class,
                       () -> runProcessStreamer(() -> ProcessStreamer.sink(Stream.of("a", "b", "c"))
                                                                     .command("echo___ hello world && echo !")
                                                                     .build()));
        });
      }
    }

    /**
     * Pipe test
     */
    @Nested
    class PipeTest extends TestUtils.TestBase {
      @Test
      public void givenSort$whenDrainDataAndClose$thenOutputIsCorrectAndInOrder() {
        assertTimeout(Duration.ofMillis(1_000), () -> assertStatement(
            value(runProcessStreamer(() -> pipe(Stream.of("c", "b", "a")).command("sort").build()))
                .toBe()
                .containingElementsInOrder(List.of(containsString("a"),
                                                   containsString("b"),
                                                   containsString("c")))
                .predicate(lastElementToBe(equalTo("c")))));
      }

      @Test
      public void givenSort$whenDrain1kDataAndClose$thenOutputIsCorrectAndInOrder() {
        assertTimeout(Duration.ofMillis(1_000), () -> assertStatement(
            value(runProcessStreamer(() -> pipe(dataStream("data", 1_000), ContextualCommandInvoker.local()).command("sort").build()))
                .toBe()
                .containingElementsInOrder(List.of(containsString("997"),
                                                   containsString("998"),
                                                   containsString("999")))
                .predicate(lastElementToBe(containsString("999")))));
      }

      @Test
      public void givenSortPipedToCatN$whenDrainOneMillionLines$thenOutputIsCorrectAndInOrder() {
        int num = 180_000;
        assertTimeout(Duration.ofMillis(1_000), () -> assertStatement(
            value(runProcessStreamer(() -> pipe(dataStream("data", num))
                .command("sort | cat -n")
                .build())).invoke("size").asInteger().toBe().equalTo(num).$()));
      }

      @Test
      public void givenCatN$whenDrainData$thenOutputIsCorrectAndInOrder() {
        assertTimeout(Duration.ofMillis(1_000), () ->
            assertStatement(value(runProcessStreamer(() -> pipe(Stream.of("a", "b", "c")).command("cat -n").build()))
                                .toBe()
                                .containingElementsInOrder(List.of(containsString("a"),
                                                                   containsString("b"),
                                                                   containsString("c")))
                                .predicate(lastElementToBe(containsString("c")))));
      }

      @Test
      public void givenCat$whenDrainMediumSizeDataAndClose$thenOutputIsCorrectAndInOrder() {
        assertTimeout(Duration.ofMillis(1_000), () -> {
          int lines = 100_000;
          List<String> data = new ArrayList<>(runProcessStreamer(() -> pipe(dataStream("data", lines)).command("cat -n").build()));

          data.forEach(System.err::println);

          assertStatement(
              value(data).size()
                         .toBe()
                         .equalTo(lines));
          assertStatement(
              value(data).toBe()
                         .containingElementsInOrder(List.of(containsString("data-0"),
                                                            containsString("data-" + (lines - 2)),
                                                            containsString("data-" + (lines - 1))))
                         .predicate(lastElementToBe(containsString("data-" + (lines - 1)))));
        });
      }

      @Test
      public void givenCatWithMinimumQueueAndRingBufferSize$whenDrainDataAndClose$thenOutputIsCorrectAndInOrder() {
        assertTimeout(Duration.ofMillis(1_000), () -> assertStatement(
            value(runProcessStreamer(
                () -> ProcessStreamer.pipe(Stream.of("a", "b", "c", "d", "e", "f", "g", "h"))
                                     .command("cat -n")
                                     .queueSize(1)
                                     .ringBufferSize(1)
                                     .build()))
                .toBe()
                .containingElementsInOrder(List.of(containsString("a"),
                                                   containsString("b"),
                                                   containsString("c"),
                                                   containsString("h")))
                .predicate(lastElementToBe(containsString("h")))));
      }

      @Test
      public void pipeTest() {
        assertTimeout(Duration.ofMillis(1_000), () -> {
          ProcessStreamer ps = pipe(dataStream("A", 10_000))
              .command("cat -n")
              .queueSize(1)
              .build();
          ps.stream().forEach(System.out::println);
        });
      }

      @Test
      public void pipeTest100_000() {
        assertTimeout(Duration.ofMillis(10_000), () -> {
          ProcessStreamer ps = pipe(dataStream("A", 100_000)).command("cat -n").build();
          //noinspection resource
          Executors.newSingleThreadExecutor().submit(() -> {
          });
          ps.stream().forEach(System.out::println);
        });
      }
    }

    @Nested
    public class CheckerTest extends TestUtils.TestBase {
      @Test
      public void givenInvalidCharacters$whenRunProcessStreamer$thenOutputsExpectedErrorMessages() {
        assertThrows(ProcessStreamer.Failure.class, () -> {
          String command = "echo test";
          ProcessStreamer ps = source()
              .command(command)
              .checker(new TestChecker())
              .build();

          try (Stream<String> s = ps.stream()) {
            s.forEach(System.out::println);
          } catch (ProcessStreamer.Failure e) {
            assertThat(
                e.getMessage(),
                allOf(
                    containsString("Detecting an issue in stdOut"),
                    containsString("Detecting an issue in stdErr")
                ));
            throw e;
          }
        });
      }

      public static class TestChecker implements ProcessStreamer.Checker {

        @Override
        public StreamChecker forStdOut() {
          return new StreamChecker() {
            private boolean flag = false;

            @Override
            public boolean getAsBoolean() {
              return flag;
            }

            @Override
            public void accept(String s) {
              if (s.contains("test")) {
                flag = true;
              }
            }

            @Override
            public String toString() {
              return "Detecting an issue in stdOut";
            }
          };
        }

        @Override
        public StreamChecker forStdErr() {
          return new StreamChecker() {
            private boolean flag = false;

            @Override
            public boolean getAsBoolean() {
              return flag;
            }

            @Override
            public void accept(String s) {
              if (s.contains("test")) {
                flag = true;
              }
            }

            @Override
            public String toString() {
              return "Detecting an issue in stdErr";
            }
          };
        }

        @Override
        public Predicate<Integer> exitCodeChecker() {
          return i -> i == 0;
        }
      }
    }

    private static List<String> runProcessStreamer(Supplier<ProcessStreamer> processStreamerSupplier)
        throws InterruptedException {
      ProcessStreamer ps = processStreamerSupplier.get();
      List<String> out = new LinkedList<>();
      try (Stream<String> stdout = ps.stream()) {
        stdout.forEach(out::add);
      }
      System.out.println("pid:" + ps.getPid() + "=exitCode:" + ps.waitFor());
      return out;
    }
  }
}