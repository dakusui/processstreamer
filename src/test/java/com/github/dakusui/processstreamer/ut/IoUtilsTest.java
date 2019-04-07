package com.github.dakusui.processstreamer.ut;

import com.github.dakusui.processstreamer.utils.IoUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static com.github.dakusui.crest.Crest.allOf;
import static com.github.dakusui.crest.Crest.asInteger;
import static com.github.dakusui.crest.Crest.asString;
import static com.github.dakusui.crest.Crest.assertThat;

public class IoUtilsTest {
  @Test
  public void longerLineThanMaxBytesPerLine() throws IOException {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 4096; i++) {
      b.append((char) ('0' + ((char) i % 10)));
    }
    String longLine = b.toString();
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(longLine.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(1).$(),
            asString("get", 0).equalTo(longLine).$()
        )
    );
  }

  private static Predicate<Integer> equalToOne() {
    return new Predicate<Integer>() {
      @Override
      public boolean test(Integer integer) {
        return Objects.equals(integer, 1);
      }

      @Override
      public String toString() {
        return "equalTo[1]";
      }
    };
  }
}
