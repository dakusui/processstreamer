package com.github.dakusui.processstreamer.pipeline;

import com.github.dakusui.processstreamer.core.process.ProcessStreamer;
import com.github.dakusui.processstreamer.core.process.Shell;
import com.github.dakusui.processstreamer.core.stream.Merger;
import com.github.dakusui.processstreamer.core.stream.Partitioner;
import com.github.dakusui.processstreamer.core.stream.Tee;
import com.github.dakusui.processstreamer.utils.StreamUtils;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.BaseStream;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * A wrapper of a process streamer's builder.
 */
public interface Pipeline {
  /**
   * Connects a given stream to {@code stdin}
   *
   * @param stdin A stream to be connected.
   * @return This object.
   */
  Pipeline stdin(Stream<String> stdin);

  /**
   * Returns a stream connected to this pipeline's {@code stdin}.
   * Or {@code null}, if not connected any.
   *
   * @return A connected stream to {@code stdin},
   */
  Stream<String> stdin();

  /**
   * Connects {@code pipelines} to downstream side of this pipeline.
   * The {@code Stream<String>} returned by {@link Pipeline#stream()} method will be
   * {@code connect}'ed to them by using {@link Tee} class.
   * However, if an element in {@code pielines} returns non-{@code null} stream when
   * {@code stream()} method is called, the stream used for the element's {@code stdio}.
   *
   * @param pipelines downstream pipelines.
   * @return This pipeline.
   */
  Pipeline connect(Pipeline... pipelines);

  /**
   * Returns a {@code Stream<String>} object. The stream must be closed by a user.
   *
   * @return A stream
   */
  Stream<String> stream();

  default Pipeline map(Pipeline mapper) {
    return this.mapStream(
        in -> mapper.stdin(in).stream().onClose(in::close)
    );
  }

  Pipeline mapStream(Function<Stream<String>, Stream<String>> mapper);

  default Pipeline map(Function<String, String> mapper) {
    return mapStream(in -> in.map(mapper));
  }

  interface Factory {
    default Pipeline pipeline(String cmd) {
      requireNonNull(cmd);
      return pipeline(CommandLineComposer.create(cmd));
    }

    default Pipeline pipeline(CommandLineComposer commandLineComposer) {
      return new Impl(shell(), requireNonNull(commandLineComposer), numPartitions());
    }

    default Shell shell() {
      return Shell.local();
    }

    default int numPartitions() {
      return 1;
    }
  }

  @FunctionalInterface
  interface CommandLineComposer extends Supplier<String> {
    default String compose() {
      return requireNonNull(this.get());
    }

    static CommandLineComposer create(String commandLine) {
      return create(commandLine, () -> commandLine);
    }

    static CommandLineComposer create(String description, Supplier<String> commandLineSupplier) {
      return new CommandLineComposer() {
        @Override
        public String get() {
          return commandLineSupplier.get();
        }

        @Override
        public String toString() {
          return description;
        }
      };
    }
  }

  class Impl implements Pipeline {
    final         ProcessStreamer.Builder builder;
    private final int                     numPartitions;
    Stream<String>                           stdin;
    /**
     * A nested function that represents a sequence of actions performed on the stream
     * of the {@code ProcessStreamer} built by this object.
     */
    Function<Stream<String>, Stream<String>> actions = Function.identity();

    Impl(Shell shell, CommandLineComposer commandLineComposer, int numPartitions) {
      this.builder = ProcessStreamer.source(shell).command(commandLineComposer.compose());
      this.numPartitions = numPartitions;
    }

    public Pipeline stdin(Stream<String> stdin) {
      this.stdin = stdin;
      return this;
    }

    public Stream<String> stdin() {
      return this.stdin;
    }

    @Override
    public Pipeline mapStream(Function<Stream<String>, Stream<String>> mapper) {
      if (this.numPartitions == 1)
        actions = actions.andThen(mapper);
      else
        actions = actions.andThen(
            partition(
                this.numPartitions,
                mapper,
                Object::hashCode));
      return this;
    }

    @Override
    public Pipeline connect(Pipeline... pipelines) {
      actions = actions.andThen(
          tee(pipelines.length, new Function<Stream<String>, Stream<String>>() {
            AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Stream<String> apply(Stream<String> stream) {
              Pipeline pipeline = pipelines[counter.getAndIncrement()];
              return pipeline.stdin() == null ?
                  pipeline.stdin(stream).stream() :
                  pipeline.stream();
            }
          }));
      return this;
    }

    @Override
    public Stream<String> stream() {
      final Stream<String> up;
      return StreamUtils.closeOnFinish(
          this.actions.apply(up = this.builder
              .stdin(stdin)
              .build()
              .stream())
              .onClose(up::close));
    }

    private Function<Stream<String>, Stream<String>> tee(
        int numSplits,
        Function<Stream<String>, Stream<String>> streamMapper) {
      return stream -> {
        List<Stream<String>> splits;
        return new Merger.Builder<>(
            (splits = new Tee.Builder<>(stream)
                .numQueues(numSplits)
                .build().tee()
                .stream()
                .map(streamMapper)
                .collect(toList())))
            .build()
            .merge()
            .onClose(() -> splits.forEach(BaseStream::close));
      };
    }

    private Function<Stream<String>, Stream<String>> partition(
        int numPartitions,
        Function<Stream<String>, Stream<String>> streamMapper,
        Function<String, Integer> partitioningFunction
    ) {
      return stream -> {
        List<Stream<String>> splits;
        return new Merger.Builder<>(
            (splits = new Partitioner.Builder<>(stream)
                .numQueues(numPartitions)
                .partitioningFunction(partitioningFunction)
                .build()
                .partition()
                .stream()
                .map(streamMapper)
                .collect(toList())))
            .build()
            .merge()
            .onClose(() -> splits.forEach(BaseStream::close));
      };
    }
  }
}
