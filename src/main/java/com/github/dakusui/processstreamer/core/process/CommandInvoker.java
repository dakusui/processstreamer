package com.github.dakusui.processstreamer.core.process;

import com.github.dakusui.processstreamer.utils.StreamUtils;

import java.util.*;
import java.util.stream.Stream;

import static com.github.valid8j.classic.Requires.requireNonNull;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

public interface CommandInvoker {
  
  CommandInvoker LOCAL_COMMAND_INVOKER = new Builder.ForLocal().build();
  
  String program();
  
  List<String> options();
  
  List<String> composeCommandLine(List<String> commandLine);
  
  default String format() {
    return String.format("%s", concat(Stream.of(program()),
                                      options().stream())
        .collect(joining(" ")));
  }
  
  static CommandInvoker local() {
    return LOCAL_COMMAND_INVOKER;
  }
  
  static CommandInvoker empty() {
    return new PrefixStyle(null, Collections.emptyList());
  }
  
  static CommandInvoker prefixStyle(String command, String... options) {
    return new PrefixStyle(command, asList(options));
  }
  
  static CommandInvoker ssh(String user, String host) {
    return ssh(user, host, null);
  }
  
  static CommandInvoker ssh(String user, String host, String identity) {
    return new Builder.ForSsh(host).userName(user).identity(identity).build();
  }
  
  
  class ExecutionContextStyle implements CommandInvoker {
    private final String program;
    private final List<String> options;
    
    ExecutionContextStyle(String program, List<String> options) {
      this.program = program;
      this.options = options;
    }
    
    @Override
    public List<String> composeCommandLine(List<String> commandLine) {
      return StreamUtils.concat(Stream.of(this.program()).filter(Objects::nonNull),
                                this.options().stream(),
                                Stream.of(String.join(" ", commandLine)))
                        .toList();
    }
    
    @Override
    public String program() {
      return program;
    }
    
    @Override
    public List<String> options() {
      return options;
    }
    
    
    @Override
    public String toString() {
      return format();
    }
  }
  
  record PrefixStyle(String program, List<String> options) implements CommandInvoker {
    public PrefixStyle(String program, List<String> options) {
      this.program = program;
      this.options = new ArrayList<>(requireNonNull(options));
    }
    
    @Override
    public List<String> options() {
      return unmodifiableList(this.options);
    }
    
    public List<String> composeCommandLine(List<String> commandLine) {
      return StreamUtils.concat(Stream.of(this.program()).filter(Objects::nonNull),
                                this.options().stream(),
                                commandLine.stream())
                        .toList();
    }
  }
  
  @SuppressWarnings("WeakerAccess")
  abstract class Builder<B extends Builder<B>> {
    private String program;
    private final List<String> options = new LinkedList<>();
    
    @SuppressWarnings("unchecked")
    public B withProgram(String program) {
      this.program = Objects.requireNonNull(program);
      return (B) this;
    }
    
    @SuppressWarnings("unchecked")
    public B clearOptions() {
      this.options.clear();
      return (B) this;
    }
    
    @SuppressWarnings("unchecked")
    public B addOption(String option) {
      this.options.add(option);
      return (B) this;
    }
    
    @SuppressWarnings("unchecked")
    public B addOption(@SuppressWarnings("SameParameterValue") String option, String value) {
      this.options.add(option);
      this.options.add(value);
      return (B) this;
    }
    
    String getProgram() {
      return this.program;
    }
    
    List<String> getOptions() {
      return this.options;
    }
    
    public CommandInvoker build() {
      Objects.requireNonNull(this.program);
      return new ExecutionContextStyle(getProgram(), this.getOptions());
    }
    
    @SuppressWarnings("WeakerAccess")
    public static class ForLocal extends Builder<Builder.ForLocal> {
      public ForLocal() {
        this.withProgram("sh")
            .addOption("-c");
      }
    }
    
    @SuppressWarnings("WeakerAccess")
    public static class ForSsh extends Builder<Builder.ForSsh> {
      private String userName;
      private final String hostName;
      private String identity = null;
      
      public ForSsh(String hostName) {
        this.hostName = Objects.requireNonNull(hostName);
        this.withProgram("ssh")
            .addOption("-o", "PasswordAuthentication=no")
            .addOption("-o", "StrictHostkeyChecking=no");
      }
      
      public ForSsh userName(String userName) {
        this.userName = userName;
        return this;
      }
      
      public ForSsh identity(String identity) {
        this.identity = identity;
        return this;
      }
      
      List<String> getOptions() {
        return concat(
            super.getOptions().stream(),
            concat(
                composeIdentity().stream(),
                Stream.of(
                    composeAccount()
                )
            )
        ).collect(toList());
      }
      
      List<String> composeIdentity() {
        return identity == null ?
               Collections.emptyList() :
               asList("-i", identity);
      }
      
      String composeAccount() {
        return userName == null ?
               hostName :
               String.format("%s@%s", userName, hostName);
      }
    }
  }
}
