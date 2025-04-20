module com.github.dakusui.processstreamer {
    requires org.slf4j;
    requires valid8j;

    exports com.github.dakusui.processstreamer.core.process;
    exports com.github.dakusui.processstreamer.launchers;
    exports com.github.dakusui.processstreamer.exceptions;
    exports com.github.dakusui.processstreamer.utils;
}