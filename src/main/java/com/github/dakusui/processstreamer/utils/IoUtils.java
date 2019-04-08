package com.github.dakusui.processstreamer.utils;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public enum IoUtils {
  ;
  public static BufferedReader bufferedReader(InputStream is, Charset charset) {
    return new BufferedReader(new InputStreamReader(is, charset));
  }
}
