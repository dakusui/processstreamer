package com.github.dakusui.processstreamer.ut;


import com.github.dakusui.processstreamer.core.process.ContextualCommandInvoker;
import com.github.dakusui.processstreamer.launchers.CommandLauncher;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.github.valid8j.fluent.Expectations.assertAll;
import static com.github.valid8j.fluent.Expectations.value;

public class CommandLauncherTest {
  @Test
  public void simple() {
    List<String> out = new ArrayList<>();
    CommandLauncher.begin()
                   .command("echo")
                   .arg("hello")
                   .perform()
                   .forEach(out::add);

    assertAll(
        value(out).size().toBe().equalTo(1),
        value(out).elementAt(0).asString().toBe().equalTo("hello")
    );
  }

  @Test
  public void whenTwoArgumentsPassed_thenBothPrinted() {
    List<String> out = new ArrayList<>();
    CommandLauncher.begin()
                   .command("echo")
                   .arg("hello")
                   .arg("world")
                   .perform()
                   .forEach(out::add);

    assertAll(
        value(out).size()
                  .toBe()
                  .equalTo(1),
        value(out).elementAt(0)
                  .asString()
                  .toBe()
                  .equalTo("hello world")
    );
  }

  @Test
  public void _whenTwoArgumentsPassed_thenBothPrinted() {
    List<String> out = new ArrayList<>();
    CommandLauncher.begin()
                   .shell(ContextualCommandInvoker.prefixStyle("time"))
                   .command("echo")
                   .arg("hello")
                   .arg("world")
                   .perform()
                   .forEach(out::add);

    assertAll(
        value(out).size().toBe().greaterThan(1),
        value(out).toBe().containing("hello world")
    );
  }
}
