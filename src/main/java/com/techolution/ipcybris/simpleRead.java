package com.techolution.ipcybris;

import org.apache.beam.runners.dataflow.DataflowPipelineRegistrar;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.runners.dataflow.TestDataflowPipelineOptions;

public class simpleRead {

  public static void main(String args[])
  {
    PipelineOptions options=PipelineOptionsFactory.create();
    Pipeline p=Pipeline.create(options);

    p.apply(TextIO.read().from("gs://uspto_data/RE.zip"))
                        .apply(TextIO.write().to("gs://uspto_data/RE2/"));
        p.run(options);

  }


}
