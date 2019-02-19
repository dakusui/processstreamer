package com.github.dakusui.processstreamer.exceptions;

public class CommandExecutionException extends CommandException {
  public CommandExecutionException(String msg, Throwable t) {
    super(msg, t);
  }
}
