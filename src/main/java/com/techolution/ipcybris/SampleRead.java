package com.techolution.ipcybris;

import com.google.common.annotations.VisibleForTesting;

import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.beam.repackaged.beam_sdks_java_core.org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.io.FilenameUtils;

/**
 * This pipeline decompresses file(s) from Google Cloud Storage and re-uploads them to a destination
 * location.
 *
 * <p><b>Parameters</b>
 *
 * <p>The {@code --inputFilePattern} parameter specifies a file glob to process. Files found can be
 * expressed in the following formats:
 *
 * <pre>
 * --inputFilePattern=gs://bucket-name/compressed-dir/*
 * --inputFilePattern=gs://bucket-name/compressed-dir/demo*.gz
 * </pre>
 *
 * <p>The {@code --outputDirectory} parameter can be expressed in the following formats:
 *
 * <pre>
 * --outputDirectory=gs://bucket-name
 * --outputDirectory=gs://bucket-name/decompressed-dir
 * </pre>
 *
 * <p>The {@code --outputFailureFile} parameter indicates the file to write the names of the files
 * which failed decompression and their associated error messages. This file can then be used for
 * subsequent processing by another process outside of Dataflow (e.g. send an email with the
 * failures, etc.). If there are no failures, the file will still be created but will be empty. The
 * failure file structure contains both the file that caused the error and the error message in CSV
 * format. The file will contain one header row and two columns (Filename, Error). The filename
 * output to the failureFile will be the full path of the file for ease of debugging.
 *
 * <pre>
 * --outputFailureFile=gs://bucket-name/decompressed-dir/failed.csv
 * </pre>
 *
 * <p>Example Output File:
 *
 * <pre>
 * Filename,Error
 * gs://docs-demo/compressedFile.gz, File is malformed or not compressed in BZIP2 format.
 * </pre>
 *
 * <p><b>Example Usage</b>
 *
 * <pre>
 * mvn compile exec:java \
 * -Dexec.mainClass=com.google.cloud.teleport.templates.BulkDecompressor \
 * -Dexec.cleanupDaemonThreads=false \
 * -Dexec.args=" \
 * --project=${PROJECT_ID} \
 * --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
 * --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
 * --runner=DataflowRunner \
 * --inputFilePattern=gs://${PROJECT_ID}/compressed-dir/*.gz \
 * --outputDirectory=gs://${PROJECT_ID}/decompressed-dir \
 * --outputFailureFile=gs://${PROJECT_ID}/decompressed-dir/failed.csv"
 * </pre>
 */
public class SampleRead {

  /** The logger to output status messages to. */
//    private static final Logger LOG = LoggerFactory.getLogger(UnzipParent.class);

  /**
   * A list of the {@link Compression} values excluding {@link Compression#AUTO} and {@link
   * Compression#UNCOMPRESSED}.
   */
  @VisibleForTesting
  static final Set<Compression> SUPPORTED_COMPRESSIONS =
          Stream.of(Compression.values())
                  .filter(value -> value != Compression.AUTO && value != Compression.UNCOMPRESSED)
                  .collect(Collectors.toSet());

  /**
   * The error msg given when the pipeline matches a file but cannot determine the compression.
   */
  @VisibleForTesting
  static final String UNCOMPRESSED_ERROR_MSG =
          "Skipping file %s because it did not match any compression mode (%s)";

  @VisibleForTesting
  static final String MALFORMED_ERROR_MSG =
          "The file resource %s is malformed or not in %s compressed format.";

  /**
   * The tag used to identify the main output of the {@link Decompress} DoFn.
   */
  @VisibleForTesting
  static final TupleTag<String> DECOMPRESS_MAIN_OUT_TAG = new TupleTag<String>() {
  };

  /**
   * The tag used to identify the dead-letter sideOutput of the {@link Decompress} DoFn.
   */
  @VisibleForTesting
  static final TupleTag<KV<String, String>> DEADLETTER_TAG = new TupleTag<KV<String, String>>() {
  };

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions {
    @Description("The input file pattern to read from (e.g. gs://bucket-name/compressed/*.gz)")
    @Validation.Required
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Description("The output location to write to (e.g. gs://bucket-name/decompressed)")
    @Validation.Required
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> value);

  }

  /**
   * The main entry-point for pipeline execution. This method will start the pipeline but will not
   * wait for it's execution to finish. If blocking execution is required, use the {@link
   * BulkDecompressor#run(Options)} method to start the pipeline and invoke {@code
   * result.waitUntilFinish()} on the {@link PipelineResult}.
   *
   * @param args The command-line args passed by the executor.
   */
  public static void main(String[] args) {

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    run(options);
  }

  /**
   * Runs the pipeline to completion with the specified options. This method does not wait until the
   * pipeline is finished before returning. Invoke {@code result.waitUntilFinish()} on the result
   * object to block until the pipeline is finished running if blocking programmatic execution is
   * required.
   *
   * @param options The execution options.
   * @return The pipeline result.
   */
  public static PipelineResult run(Options options) {

    // Create the pipeline
    Pipeline pipeline = Pipeline.create(options);
    ValueProvider output = options.getOutputDirectory();

    // Run the pipeline over the work items.
    pipeline.apply("MatchFile(s)", FileIO.match().filepattern(options.getInputFilePattern()))
            //.apply("DecompressFile(s)", ParDo.of(new DecompressNew(options.getOutputDirectory())))
            .apply(FileIO.readMatches().withCompression(Compression.AUTO))
            .apply(FileIO.write().to(output));

    return pipeline.run();
  }
}

