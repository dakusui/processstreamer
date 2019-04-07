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
import static com.github.dakusui.processstreamer.utils.IoUtils.BoundedBufferedReader.DEFAULT_MAX_LINE_LENGTH;

public class IoUtilsTest {
  @Test
  public void longerLineThanMaxBytesPerLine() throws IOException {
    String data = createString(4096);
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(1).$(),
            asString("get", 0).equalTo(data).$()
        )
    );
  }

  @Test
  public void shorterLineThanMaxBytesPerLine() throws IOException {
    String data = createString(100);
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(1).$(),
            asString("get", 0).equalTo(data).$()
        )
    );
  }

  @Test
  public void shorterLineThanMaxBytesPerLineByOneByte() throws IOException {
    String data = createString(DEFAULT_MAX_LINE_LENGTH - 1);
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(1).$(),
            asString("get", 0).equalTo(data).$()
        )
    );
  }

  @Test
  public void lineWhoseLengthIsEqualToMaxBytesPerLine() throws IOException {
    String data = createString(1024);
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(1).$(),
            asString("get", 0).equalTo(data).$()
        )
    );
  }

  @Test
  public void longerLineThanMaxBytesPerLineByOneByte() throws IOException {
    String data = createString(DEFAULT_MAX_LINE_LENGTH + 1);
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(1).$(),
            asString("get", 0).equalTo(data).$()
        )
    );
  }

  @Test
  public void emptyLine() throws IOException {
    String data = "";
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(0).$()
        )
    );
  }

  @Test
  public void emptyLines() throws IOException {
    String data = String.format("%n");
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(1).$(),
            asString("get", 0).equalTo("").$()
        )
    );
  }

  @Test
  public void lineAfterEmptyLine() throws IOException {
    String data = String.format("%nhello");
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(2).$(),
            asString("get", 0).equalTo("").$(),
            asString("get", 1).equalTo("hello").$()
        )
    );
  }

  @Test
  public void twoLinesA() throws IOException {
    String l1 = createString(DEFAULT_MAX_LINE_LENGTH - 1);
    String l2 = createString(1024);
    String data = String.format("%s%n%s", l1, l2);
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(2).$(),
            asString("get", 0).equalTo(l1).$(),
            asString("get", 1).equalTo(l2).$()
        )
    );
  }

  @Test
  public void twoLinesB() throws IOException {
    String l1 = createString(1024);
    String l2 = createString(DEFAULT_MAX_LINE_LENGTH + 1);
    String data = String.format("%s%n%s", l1, l2);
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(2).$(),
            asString("get", 0).equalTo(l1).$(),
            asString("get", 1).equalTo(l2).$()
        )
    );
  }

  @Test
  public void lines() throws IOException {
    String l1 = createString(DEFAULT_MAX_LINE_LENGTH - 1);
    String l2 = createString(DEFAULT_MAX_LINE_LENGTH);
    String l3 = createString(DEFAULT_MAX_LINE_LENGTH + 1);
    String data = String.format("%s%n%s%n%s", l1, l2, l3);
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(3).$(),
            asString("get", 0).equalTo(l1).$(),
            asString("get", 1).equalTo(l2).$(),
            asString("get", 2).equalTo(l3).$()
        )
    );
  }

  @Test
  public void shortLines() throws IOException {
    String l1 = createString(10);
    String l2 = createString(10);
    String l3 = createString(10);
    String data = String.format("%s%n%s%n%s", l1, l2, l3);
    List<String> readLines = new LinkedList<>();
    BufferedReader r = IoUtils.bufferedReader(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())), Charset.defaultCharset());
    String line;
    while ((line = r.readLine()) != null) {
      readLines.add(line);
    }
    assertThat(
        readLines,
        allOf(
            asInteger("size").equalTo(3).$(),
            asString("get", 0).equalTo(l1).$(),
            asString("get", 1).equalTo(l2).$(),
            asString("get", 2).equalTo(l3).$()
        )
    );
  }

  private String createString(int length) {
    StringBuilder b = new StringBuilder();
    for (int j = 0; j < length; j++) {
      b.append((char) ('0' + ((char) j % 10)));
    }
    return b.toString();
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
