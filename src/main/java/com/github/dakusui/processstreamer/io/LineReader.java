package com.github.dakusui.processstreamer.io;

import com.github.dakusui.processstreamer.exceptions.CommandException;

public interface LineReader {
	String read() throws CommandException;

	void close() throws CommandException;
}
