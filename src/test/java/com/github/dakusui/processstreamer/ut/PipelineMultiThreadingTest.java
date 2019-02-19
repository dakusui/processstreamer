package com.github.dakusui.processstreamer.ut;

public class PipelineMultiThreadingTest extends PipelineTest {
  @Override
  public int numPartitions() {
    return 8;
  }
}
