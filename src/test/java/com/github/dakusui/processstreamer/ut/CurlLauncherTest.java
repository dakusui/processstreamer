package com.github.dakusui.processstreamer.ut;

import com.github.dakusui.processstreamer.launchers.CurlLauncher;
import com.github.dakusui.processstreamer.ututils.TestUtils;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;

import static com.github.valid8j.fluent.Expectations.assertAll;
import static com.github.valid8j.fluent.Expectations.value;
import static com.github.valid8j.pcond.forms.Predicates.containsString;

public class CurlLauncherTest extends TestUtils.TestBase {
  public static HttpServer server;

  @BeforeAll
  static void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0); // 0 for random port
    server.createContext("/hello", exchange -> {
      String response = "Hello, world!";
      exchange.sendResponseHeaders(200, response.length());
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(response.getBytes());
      }
    });
    server.start();
  }

  @Test
  public void _curlTest() {
    int port = server.getAddress().getPort();
    List<String> out = CurlLauncher.begin()
                                   .arg("http://localhost:" + port + "/hello")
                                   .perform()
                                   .peek(System.out::println)
                                   .toList();
    assertAll(
        value(out).size().toBe().greaterThan(0),
        value(out).toBe().containingElementsInOrder(List.of(containsString("Hello, world!"))));
  }

  @Test
  public void _curlTest2() {
    List<String> out = CurlLauncher.begin()
                                   .option("-V")
                                   .perform()
                                   .peek(System.out::println)
                                   .toList();
    assertAll(
        value(out).size().toBe().greaterThan(0),
        value(out).elementAt(0).asString().toBe().containing("curl"));
  }

  @AfterAll
  static void stopServer() {
    server.stop(0);
  }
}
