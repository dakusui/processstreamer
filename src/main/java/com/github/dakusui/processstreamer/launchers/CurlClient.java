package com.github.dakusui.processstreamer.launchers;

import com.github.dakusui.processstreamer.core.process.ContextualCommandInvoker;

public class CurlClient {

  public static Builder begin() {
    return new Builder();
  }

  public static class Builder extends CommandLauncher.Builder<Builder> {
    public Builder() {
      this.shell(ContextualCommandInvoker.empty()).command("curl");
    }

    public Builder url(String url) {
      return this.arg(url);
    }
  }
}
