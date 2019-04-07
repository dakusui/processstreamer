package com.github.dakusui.processstreamer.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.concurrent.atomic.AtomicBoolean;

public enum IoUtils {
  ;

  /**
   * The BoundedBufferedReader class
   *
   * A BufferedReader that prevents DoS attacks by providing bounds for line length and number of lines
   *
   * Copyright (c) 2011 - Sean Malone
   *
   * The BoundedBufferedReader is published by Sean Malone under the BSD license. You should read and accept the
   * LICENSE before you use, modify, and/or redistribute this software.
   *
   * @author Sean Malone <sean@seantmalone.com>
   * @version 1.1
   * https://github.com/seantmalone/BoundedBufferedReader/blob/master/BoundedBufferedReader.java
   * <pre>
   *
   * </pre>
   */
  public static BufferedReader bufferedReader(InputStream is, Charset charset) {
    return new BoundedBufferedReader(new InputStreamReader(is, charset));
  }

  public static class BoundedBufferedReader extends BufferedReader {
    public static final int DEFAULT_MAX_LINE_LENGTH = 1024;        //Max bytes per line

    private final int     readerMaxLineLen;
    private       boolean finished;

    BoundedBufferedReader(InputStreamReader reader) {
      super(reader);
      readerMaxLineLen = DEFAULT_MAX_LINE_LENGTH;
      finished = false;
    }

    @Override
    public String readLine() throws IOException {
      if (finished)
        return null;
      AtomicBoolean tooLong = new AtomicBoolean(false);
      String line = privateReadLine(tooLong);
      if (line == null)
        return null;
      if (!tooLong.get())
        return line;
      StringBuilder b = new StringBuilder(readerMaxLineLen * 4);
      b.append(line);
      do {
        tooLong.set(false);
        line = privateReadLine(tooLong);
        if (line == null) {
          finished = true;
          return b.toString();
        }
        b.append(line);
      } while (tooLong.get());
      return b.toString();
    }

    private String privateReadLine(AtomicBoolean tooLong) throws IOException {
      int currentPos = 0;
      char[] data = new char[readerMaxLineLen];
      final int CR = 13;
      final int LF = 10;
      int currentCharVal = super.read();

      //Read characters and add them to the data buffer until we hit the end of a line or the end of the file.
      while ((currentCharVal != CR) && (currentCharVal != LF) && (currentCharVal >= 0)) {
        data[currentPos++] = (char) currentCharVal;
        //Check readerMaxLineLen limit
        if (currentPos < readerMaxLineLen)
          currentCharVal = super.read();
        else {
          tooLong.set(true);
          break;
        }
      }

      if (currentCharVal < 0) {
        //End of file
        if (currentPos > 0)
          //Return last line
          return (new String(data, 0, currentPos));
        else
          return null;
      } else {
        //Remove newline characters from the buffer
        if (currentCharVal == CR) {
          //Check for LF and remove from buffer
          super.mark(1);
          if (super.read() != LF)
            super.reset();
        } else if (currentCharVal != LF) {
          //readerMaxLineLen has been hit, but we still need to remove newline characters.
          super.mark(1);
          int nextCharVal = super.read();
          if (nextCharVal == CR) {
            super.mark(1);
            if (super.read() != LF)
              super.reset();
          } else if (nextCharVal != LF)
            super.reset();
        }
        return (new String(data, 0, currentPos));
      }
    }
  }
}
