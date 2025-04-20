package com.github.dakusui.processstreamer.launchers;

import com.github.dakusui.processstreamer.core.process.CommandInvoker;

public class CurlClient {
  
  public static Builder begin() {
    return new Builder();
  }
  
  public static class Builder extends CommandLauncher.Builder<Builder> {
    public Builder() {
      this.shell(CommandInvoker.local()).command("curl");
    }
    
    public Builder url(String url) {
      return this.arg(url);
    }
  }
}
