package com.techolution.ipcybris;

import org.apache.beam.runners.dataflow.DataflowPipelineRegistrar;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.runners.dataflow.TestDataflowPipelineOptions;

public class simpleRead {

  public interface Options extends PipelineOptions {
    @Description("The input file pattern to read from (e.g. gs://bucket-name/compressed/*.gz)")
    @Validation.Required
    ValueProvider<String> getInputFilePattern();

    void setInputFilePattern(ValueProvider<String> value);


    @Description("The output location to write to (e.g. gs://bucket-name/decompressed)")
    @Validation.Required
    ValueProvider<String> getOutputDirectory();

    void setOutputDirectory(ValueProvider<String> value);

    @Description("The name of the topic which data should be published to. "
            + "The name should be in the format of projects/<project-id>/topics/<topic-name>.")
    @Validation.Required
    ValueProvider<String> getOutputTopic();
    void setOutputTopic(ValueProvider<String> value);
  }

  public static void main(String args[])
  {
    Options options=PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    run(options);
  }

  public static PipelineResult run(Options options){
    Pipeline p=Pipeline.create(options);
    p.apply(TextIO.read().from(options.getInputFilePattern()))
            .apply(TextIO.write().to(options.getOutputDirectory()));
    
    return p.run();
  }
}
