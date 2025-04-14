package com.github.dakusui.processstreamer.launchers;

import com.github.dakusui.processstreamer.core.process.ContextualCommandInvoker;
import com.github.dakusui.processstreamer.core.process.ProcessStreamer;
import com.github.dakusui.processstreamer.utils.StreamUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class CommandLauncher {
  private static final Logger LOGGER = LoggerFactory.getLogger(CommandLauncher.class);
  private final File directory;
  private final ContextualCommandInvoker contextualCommandInvoker;
  private final String command;
  private final List<CommandLauncherOption> options;
  private final List<String> args;

  CommandLauncher(File directory,
                  ContextualCommandInvoker contextualCommandInvoker,
                  String command,
                  List<CommandLauncherOption> options,
                  List<String> args) {
    this.directory = directory;
    this.contextualCommandInvoker = contextualCommandInvoker;
    this.command = command;
    this.options = options;
    this.args = args;
  }

  public static Builder<?> begin() {
    return new Builder<>();
  }

  public Stream<String> perform() {
    List<String> commandLine = composeCommandLine();
    LOGGER.debug("shell:<{}>, command:<{}>, directory:<{}>", this.contextualCommandInvoker, commandLine, this.directory);
    return new ProcessStreamer.Builder(this.contextualCommandInvoker, commandLine)
        .cwd(this.directory)
        .build()
        .stream();
  }

  private List<String> composeCommandLine() {
    return StreamUtils.concat(Stream.of(this.command),
                              this.options.stream().map(CommandLauncherOption::toString),
                              this.args.stream())
                      .toList();
  }

  public static class Builder<B extends Builder<B>> {
    File directory;
    public ContextualCommandInvoker contextualCommandInvoker;
    public String command;
    private final List<CommandLauncherOption> options = new ArrayList<>();
    private final List<String> args = new ArrayList<>();

    public Builder() {
      this.shell(ContextualCommandInvoker.local());
    }

    @SuppressWarnings("unchecked")
    public B shell(ContextualCommandInvoker contextualCommandInvoker) {
      this.contextualCommandInvoker = contextualCommandInvoker;
      return (B) this;
    }

    /**
     * By giving `null` you can run the command in the current directory.
     *
     * @param directory a directory in which the command is run.
     * @return This object.
     */
    @SuppressWarnings("unchecked")
    public B directory(File directory) {
      this.directory = directory;
      return (B) this;
    }

    public B shell(String shellCommand) {
      return this.shell(new ContextualCommandInvoker.Builder.ForLocal().clearOptions()
                                                                       .withProgram(shellCommand)
                                                                       .build());
    }

    @SuppressWarnings("unchecked")
    public B command(String command) {
      this.command = command;
      return (B) this;
    }

    @SuppressWarnings("unchecked")
    public B arg(String arg) {
      this.args.add(arg);
      return (B) this;
    }

    public B option(String option) {
      return this.option(option, null);
    }

    @SuppressWarnings("unchecked")
    public B option(String option, String value) {
      this.options.add(new CommandLauncherOption(option, value));
      return (B) this;
    }

    public CommandLauncher build() {
      return new CommandLauncher(directory, contextualCommandInvoker, command, this.options, this.args);
    }

    public Stream<String> perform() {
      return build().perform();
    }
  }
}
