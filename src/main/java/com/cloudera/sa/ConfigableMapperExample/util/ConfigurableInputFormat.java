package com.cloudera.sa.ConfigableMapperExample.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.split.JobSplitWriter;

public class ConfigurableInputFormat extends InputFormat<LongWritable, Text> {

  /**
   * An input split consisting of a range on numbers.
   */
  static class ConfigurableInputSplit extends InputSplit  implements Writable
  {

    long firstRow = 0;

    
    public ConfigurableInputSplit() {
      System.out.println("ConfigurableInputSplit construct");
    }


    public void write(DataOutput out) throws IOException {
      WritableUtils.writeVLong(out, firstRow);
      
    }

    public void readFields(DataInput in) throws IOException {
      firstRow = WritableUtils.readVLong(in);
      
    }

    public long getLength() throws IOException {
      // TODO Auto-generated method stub
      return 0;
    }

    public String[] getLocations() throws IOException {
      return new String[]{};
    }
   
  }
  
  static class ConfigurableRecordReader extends RecordReader<LongWritable, Text>{

    long startRow = 0;
    long finishedRows = 0;
    long totalRows = 1;
    
    
    public ConfigurableRecordReader() {
      System.out.println("ConfigurableRecordReader constuct");
    }
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (finishedRows < totalRows)
      {
        finishedRows += 1;
        return true;
      } else
      {
        return false;
      }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
        InterruptedException {
      return new LongWritable();
    }

    @Override
    public Text getCurrentValue() throws IOException,
        InterruptedException {
      
      return new Text();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      
      return 0;
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {
    
    int splits = context.getConfiguration().getInt("mapred.map.tasks", 2);
    
    List<InputSplit> splitList = new ArrayList<InputSplit>();
    
    for (int i = 0; i < splits; i++) {
      splitList.add(new ConfigurableInputSplit());
    }
    
    System.out.println("p2:" + splitList);
    
    return splitList;
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    ConfigurableRecordReader reader = new ConfigurableRecordReader();
    reader.initialize((ConfigurableInputSplit)(split), context);
    
    System.out.println("p1:" + reader);
    
    return reader;
  }

  



}
