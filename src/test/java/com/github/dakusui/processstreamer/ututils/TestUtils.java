package com.github.dakusui.processstreamer.ututils;

import com.github.valid8j.pcond.forms.Printables;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public enum TestUtils {
  ;

  static final PrintStream STDOUT = System.out;
  static final PrintStream STDERR = System.err;

  public static <T, U> MatcherBuilder<T, U> matcherBuilder() {
    return MatcherBuilder.create();
  }

  public static <T, U> MatcherBuilder<T, U> matcherBuilder(String name, Function<T, U> transformer) {
    return MatcherBuilder.<T, U>create().<T, U>transform(name, transformer);
  }

  public static String base64() {
    String systemName = systemName();
    String ret;
    if ("Linux".equals(systemName)) {
      ret = "base64 -w";
    } else if ("Mac OS X".equals(systemName)) {
      ret = "base64 -b";
    } else {
      throw new RuntimeException(String.format("%s is not a supported platform.", systemName));
    }
    return ret;
  }

  public static String systemName() {
    return System.getProperty("os.name");
  }

  public static List<String> list(String prefix, int size) {
    List<String> ret = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      ret.add(String.format("%s-%s", prefix, i));
    }
    return ret;
  }

  public static Stream<String> dataStream(String prefix, int num) {
    return IntStream.range(0, num).mapToObj(i -> String.format("%s-%s", prefix, i));
  }

  public static void suppressStdOutErrIfRunUnderSurefire() {
    if (TestUtils.isRunUnderSurefire()) {
      System.setOut(new PrintStream(new OutputStream() {
        @Override
        public void write(int b) {
        }
      }));
      System.setErr(new PrintStream(new OutputStream() {
        @Override
        public void write(int b) {
        }
      }));
    }
  }

  public static void restoreStdOutErr() {
    System.setOut(STDOUT);
    System.setOut(STDERR);
  }

  public static boolean isRunUnderSurefire() {
    return System.getProperty("surefire.real.class.path") != null;
  }

  public static String userName() {
    String key = "commandstreamer.username";
    if (!System.getProperties().contains(key))
      return System.getProperty("user.name");
    return System.getProperty(key);
  }

  public static Predicate<List<String>> lastElementToBe(Predicate<String> predicate) {
    return Printables.predicate("lastElementToBe[" + predicate + "]", l -> predicate.test(l.getLast()));
  }

  /**
   * A base class for tests which writes to stdout/stderr.
   */
  public static class TestBase {
    @BeforeEach
    public void suppressStdOutErrIfRunUnderSurefire() {
      TestUtils.suppressStdOutErrIfRunUnderSurefire();
    }

    @AfterEach
    public void restoreStdOutErr() {
      TestUtils.restoreStdOutErr();
    }
  }

  public static class MatcherBuilder<V, U> {
    String predicateName = "P";
    Predicate<U> p = null;
    String functionName = "transform";
    Function<V, U> f = null;

    public static <T> MatcherBuilder<T, T> simple() {
      return new MatcherBuilder<T, T>()
          .transform("passthrough", t -> t);
    }

    public static <T, U> MatcherBuilder<T, U> create() {
      return new MatcherBuilder<>();
    }

    public MatcherBuilder<V, U> transform(String name, Function<V, U> f) {
      this.functionName = Objects.requireNonNull(name);
      this.f = Objects.requireNonNull(f);
      return this;
    }


    public static class Item<D> {
      public final String symbol;
      public final D value;

      Item(String symbol, D value) {
        this.symbol = Objects.requireNonNull(symbol);
        this.value = value;
      }

      public String toString() {
        return String.format("(%s:%s)", symbol, value);
      }
    }
  }
}
