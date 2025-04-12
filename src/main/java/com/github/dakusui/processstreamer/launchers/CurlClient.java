package com.github.dakusui.processstreamer.launchers;

import com.github.dakusui.processstreamer.core.process.ContextualCommandInvoker;
import com.github.dakusui.processstreamer.core.process.ProcessStreamer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.github.valid8j.fluent.Expectations.all;
import static com.github.valid8j.fluent.Expectations.value;
import static java.lang.String.format;

public class CurlClient {
    private final ContextualCommandInvoker contextualCommandInvoker;
    private final List<CommandLauncherOption> options;
    private final String url;
    private final String command;

    public static Builder begin() {
        return new Builder();
    }

    /**
     * If `null` is given as `url`, no URL will be included in the command line to be performed.
     * This is designed to follow the behavior observed when `curl -V` is executed.
     *
     * @param contextualCommandInvoker   A shell with which the curl command is executed.
     * @param command A cURL command. Usually just `curl`.
     * @param options A list of options given to cURL.
     * @param url     A url against which the cURL command is executed.
     *                You can give `null` to run cURL with `-V` option, which doesn't require URL.
     */
    CurlClient(ContextualCommandInvoker contextualCommandInvoker,
               String command,
               List<CommandLauncherOption> options,
               String url) {
        assert all(value(contextualCommandInvoker).toBe().notNull(),
                value(command).toBe().notNull().notEmpty());
        this.contextualCommandInvoker = contextualCommandInvoker;
        this.command = command;
        this.options = options;
        this.url = url;
    }

    public Stream<String> perform() {
        return new ProcessStreamer.Builder(this.contextualCommandInvoker, composeCommandLine())
                .build()
                .stream();
    }

    private String composeCommandLine() {
        var b = new StringBuilder();
        b.append(format("%s", this.command));
        b.append(" ");
        options.forEach(option -> b.append(option).append(" "));
        if (url != null) {
            b.append(url);
        }
        return b.toString();
    }

    public static class Builder {
        private ContextualCommandInvoker contextualCommandInvoker;
        private final List<CommandLauncherOption> options = new ArrayList<>();
        private String url;
        private String command;

        public Builder() {
            this.shell(ContextualCommandInvoker.local()).command("curl").url(null);
        }

        public Builder shell(ContextualCommandInvoker contextualCommandInvoker) {
            this.contextualCommandInvoker = contextualCommandInvoker;
            return this;
        }

        public Builder command(String curlCommand) {
            this.command = curlCommand;
            return this;
        }

        public Builder option(String option) {
            return this.option(option, null);
        }

        public Builder option(String option, String value) {
            this.options.add(new CommandLauncherOption(option, value));
            return this;
        }

        public Builder url(String url) {
            this.url = url;
            return this;
        }

        public CurlClient build() {
            return new CurlClient(this.contextualCommandInvoker, this.command, this.options, this.url);
        }

        /**
         * A shorthand for `.build().perform()`.
         *
         * @return A stream produced by the built `CurlClient`.
         */
        public Stream<String> perform() {
            return build().perform();
        }
    }

}
