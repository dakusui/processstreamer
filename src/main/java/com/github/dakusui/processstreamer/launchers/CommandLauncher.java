package com.github.dakusui.processstreamer.launchers;

import com.github.dakusui.processstreamer.core.process.ContextualCommandInvoker;
import com.github.dakusui.processstreamer.core.process.ProcessStreamer;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class CommandLauncher {
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

  public static Builder begin() {
    return new Builder();
  }

  public Stream<String> perform() {
    List<String> commandLine = composeCommandLine();
    System.out.println("shell:<" + this.contextualCommandInvoker + ">, command:<" + commandLine + ">, directory:<" + this.directory + ">");
    return new ProcessStreamer.Builder(this.contextualCommandInvoker, commandLine)
        .cwd(this.directory)
        .build()
        .stream();
  }

  private List<String> composeCommandLine() {
    return Stream.concat(Stream.of(this.command),
                         this.args.stream())
                 .toList();
  }

  public static class Builder {
    File directory;
    public ContextualCommandInvoker contextualCommandInvoker;
    public String command;
    private final List<CommandLauncherOption> options = new ArrayList<>();
    private final List<String> args = new ArrayList<>();

    public Builder() {
      this.shell(ContextualCommandInvoker.local());
    }

    public Builder shell(ContextualCommandInvoker contextualCommandInvoker) {
      this.contextualCommandInvoker = contextualCommandInvoker;
      return this;
    }

    /**
     * By giving `null` you can run the command in the current directory.
     *
     * @param directory a directory in which the command is run.
     * @return This object.
     */
    public Builder directory(File directory) {
      this.directory = directory;
      return this;
    }

    public Builder shell(String shellCommand) {
      return this.shell(new ContextualCommandInvoker.Builder.ForLocal().clearOptions()
                                                                       .withProgram(shellCommand)
                                                                       .build());
    }

    public Builder command(String command) {
      this.command = command;
      return this;
    }

    public Builder arg(String arg) {
      this.args.add(arg);
      return this;
    }

    public Builder option(String option) {
      return this.option(option, null);
    }

    public Builder option(String option, String value) {
      this.options.add(new CommandLauncherOption(option, value));
      return this;
    }

    public CommandLauncher build() {
      return new CommandLauncher(directory, contextualCommandInvoker, command, this.options, this.args);
    }

    public Stream<String> perform() {
      return build().perform();
    }

  }
}
