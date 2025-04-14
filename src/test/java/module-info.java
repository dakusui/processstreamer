module processstreamer {
  requires com.github.dakusui.processstreamer;
  requires java.net.http;
  requires jdk.httpserver;
  requires org.junit.jupiter.api;
  requires valid8j;

  exports com.github.dakusui.processstreamer.ut;
  exports com.github.dakusui.processstreamer.ututils;
  opens com.github.dakusui.processstreamer.ut;
}