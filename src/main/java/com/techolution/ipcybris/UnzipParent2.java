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
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.GcsUtil;
import org.apache.beam.sdk.util.gcsfs.GcsPath;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.commons.io.FilenameUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


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
public class UnzipParent2 {

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

  @VisibleForTesting
  static final TupleTag<KV<String, String>> DEADLETTER_TAG = new TupleTag<KV<String, String>>() {
  };

  /**
   * The {@link Options} class provides the custom execution options passed by the executor at the
   * command-line.
   */
  public interface Options extends PipelineOptions {
    @Description("The input file pattern to read from (e.g. gs://bucket-name/compressed/*.gz)")
    @Required
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);

    @Description("The output location to write to (e.g. gs://bucket-name/decompressed)")
    @Required
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> value);

    @Description("The name of the topic which data should be published to. "
            + "The name should be in the format of projects/<project-id>/topics/<topic-name>.")
    @Required
    ValueProvider<String> getOutputTopic();

    void setOutputTopic(ValueProvider<String> value);
  }


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

    // Run the pipeline over the work items.
    pipeline.apply("MatchFile(s)", FileIO.match().filepattern(options.getInputFilePattern()))
            .apply("DecompressFile(s)", ParDo.of(new DecompressNew(options.getOutputDirectory(), options.getInputFilePattern())))
            .apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()));

    return pipeline.run();
  }

  /**
   * Performs the decompression of an object on Google Cloud Storage and uploads the decompressed
   * object back to a specified destination location.
   */
  @SuppressWarnings("serial")
  public static class DecompressNew extends DoFn<MatchResult.Metadata, String> {
    private static final long serialVersionUID = 2015166770614756341L;
    private long filesUnzipped = 0;
    private final ValueProvider<String> destinationLocation;
    private final ValueProvider<String> inputFilePattern;

    Logger log=LoggerFactory.getLogger(UnzipParent2.class);

    DecompressNew(ValueProvider<String> destinationLocation, ValueProvider<String> inputFilePattern) {
      this.destinationLocation = destinationLocation;
      this.inputFilePattern = inputFilePattern;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      ResourceId p = c.element().resourceId();
      GcsUtil.GcsUtilFactory factory = new GcsUtil.GcsUtilFactory();
      GcsUtil u = factory.create(c.getPipelineOptions());
      String desPath = "";
      String randomStr = getRandomString(10);
      String file_name = p.toString();
      byte[] buffer = new byte[100000000];
      SeekableByteChannel sek = null;
      try {
        sek = u.open(GcsPath.fromUri(p.toString()));
        log.info("seekableByteChannel is created");
      } catch (IOException e) {
        e.printStackTrace();
        log.error("Error while creating seekableByteChannel:"+e);
      }
      String ext = FilenameUtils.getExtension(p.toString());
      if (ext.equalsIgnoreCase("zip")) {
        InputStream is;
        is = Channels.newInputStream(sek);
        BufferedInputStream bis = new BufferedInputStream(is);
        ZipInputStream zis = new ZipInputStream(bis);
        ZipEntry ze = null;
        try {
          ze = zis.getNextEntry();
          log.info("Next zip entry is :"+ze);
        } catch (IOException e) {
          e.printStackTrace();
          log.error("error while creating zipEntry"+ze);
        }
        desPath = this.destinationLocation.get() + randomStr + "-unzip";
        while (ze != null) {
          log.info("zipEntry is not null");
          try {
            WritableByteChannel wri = u.create(GcsPath.fromUri(this.destinationLocation.get() + randomStr + "-unzip" + ze.getName()), getType(ze.getName()));
              log.info("writableByteChannel is created");
            OutputStream os = Channels.newOutputStream(wri);
            int len;
            while ((len = zis.read(buffer)) > 0) {
              os.write(buffer, 0, len);
              log.info("writing to: "+wri);
            }
            os.flush();
            os.close();
            wri.close();
            log.info("finished writing "+ze);
            filesUnzipped++;
            log.info("files unzipped:"+filesUnzipped);
            ze = zis.getNextEntry();
          } catch (Exception e) {
            e.printStackTrace();
            log.error("Error while reading "+ze);
          }
        }
        try {
          zis.closeEntry();
          zis.close();
          log.info("ZipInputStream closed");
        } catch (IOException e) {
          e.printStackTrace();
          log.error("unable to close ZipInputStream");
        }
      } else if (ext.equalsIgnoreCase("tar")) {
        InputStream is;
        is = Channels.newInputStream(sek);
        BufferedInputStream bis = new BufferedInputStream(is);
        TarArchiveInputStream tis = new TarArchiveInputStream(bis);
        TarArchiveEntry te = null;
        try {
          te = tis.getNextTarEntry();
          log.info("Next Tar entry is :"+te);
        } catch (IOException e) {
          e.printStackTrace();
          log.error("error while creating TarEntry"+te);
        }
        desPath = this.destinationLocation.get() + randomStr + "-untar";
        while (te != null) {
          log.info("TarEntry is not null");
          try {
            WritableByteChannel wri = u.create(GcsPath.fromUri(this.destinationLocation.get() + randomStr + "-untar" + te.getName()), getType(te.getName()));
            log.info("writableByteChannel is created");
            OutputStream os = Channels.newOutputStream(wri);
            int len;
            while ((len = tis.read(buffer)) > 0) {
              os.write(buffer, 0, len);
              log.info("writing to: "+wri);
            }
            os.flush();
            os.close();
            wri.close();
            log.info("finished writing "+te);
            filesUnzipped++;
            log.info("files unzipped:"+filesUnzipped);
            te = tis.getNextTarEntry();
          } catch (IOException e) {
            e.printStackTrace();
            log.error("Error while reading "+te);
          }
          try {
            tis.close();
            log.info("TarInputStream closed");
          } catch (IOException e) {
            e.printStackTrace();
            log.error("unable to close TarInputStream");
          }
        }
      }
      String input_file_pattern = this.inputFilePattern.get();
      String[] split_pattern = input_file_pattern.split("/");
      String filename = split_pattern[split_pattern.length - 1];
      String input_name = FilenameUtils.getBaseName(filename).toLowerCase();
      JSONObject pubsubout = new JSONObject();
      pubsubout.put("parent-name", input_name);
      pubsubout.put("extraction-path", desPath);
      c.output(pubsubout.toString());
    }

    private String getType(String fName) {
      if (fName.endsWith(".zip")) {
        return "application/x-zip-compressed";
      } else if (fName.endsWith(".tar")) {
        return "application/x-tar";
      } else {
        return "text/plain";
      }
    }

    public static String getRandomString(int length) {
      char[] chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRST".toCharArray();

      StringBuilder sb = new StringBuilder();
      Random random = new Random();
      for (int i = 0; i < length; i++) {
        char c = chars[random.nextInt(chars.length)];
        sb.append(c);
      }
      String randomStr = sb.toString();

      return randomStr;
    }
  }
}
