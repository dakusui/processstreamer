package com.github.dakusui.processstreamer.launchers;

import java.util.Optional;

import static java.lang.String.format;

record CommandLauncherOption(String option, String optionValue) {
  Optional<String> value() {
    return Optional.ofNullable(optionValue);
  }
  
  @Override
  public String toString() {
    if (value().isPresent()) {
      return format("%s %s", option, optionValue);
    } else {
      return format("%s", option);
    }
  }
}
