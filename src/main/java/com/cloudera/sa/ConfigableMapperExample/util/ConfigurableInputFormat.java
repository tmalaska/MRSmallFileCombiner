package com.cloudera.sa.ConfigableMapperExample.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class ConfigurableInputFormat extends InputFormat<LongWritable, Text> {

  private static final String MAPRED_MAP_TASKS = "mapred.map.tasks";
  public static final String INPUT_PATH = "ConfigurableInputFormat.InputPath";

  /**
   * An input split consisting of a range on numbers.
   */
  static class ConfigurableInputSplit extends InputSplit implements Writable {

    long firstRow = 0;

    public ConfigurableInputSplit() {

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
      return new String[] {};
    }

  }

  static class ConfigurableRecordReader extends
      RecordReader<LongWritable, Text> {

    long startRow = 0;
    long finishedRows = -1;
    long totalRows = 0;
    
    long totalBytesToRead = 0;
    long totalBytesRead = 0;
    
    TaskAttemptContext context;
    
    boolean isSetup = false;
    
    ArrayList<FileStatus> filesToRead = new ArrayList<FileStatus>();

    public ConfigurableRecordReader() {
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      
      if (isSetup == false) {
        isSetup = true;
        
        this.context = context; 
        
        System.out.println("Setup");

        int numberOfMappers = context.getConfiguration().getInt(
            MAPRED_MAP_TASKS, -1);

        if (numberOfMappers == -1) {
          throw new RuntimeException();
        }

        String inputFolder = context.getConfiguration().get(INPUT_PATH);

        int taskId = context.getTaskAttemptID().getTaskID().getId();

        try {
          Configuration config = new Configuration();
          FileSystem hdfs = FileSystem.get(config);

          Path inputFolderPath = new Path(inputFolder);

          if (hdfs.isDirectory(inputFolderPath)) {
            FileStatus[] fileStatuses = hdfs.listStatus(inputFolderPath);

            for (FileStatus fileStatus : fileStatuses) {

              Path sourceFilePath = fileStatus.getPath();

              if (Math.abs(sourceFilePath.getName().hashCode() % numberOfMappers) == taskId) {
                //System.out.println("FileToRead:" + fileStatus.getPath());
                addFileToReadList(hdfs, fileStatus, context);
              } else {
                //System.out.println("FileNotToRead:" + fileStatus.getPath());
              }
            }

          } else {
            // We have a single file so only the first mapper can work
            // on it
            if (taskId == 0) {
              FileStatus fileStatus = hdfs.getFileStatus(inputFolderPath);
              
              addFileToReadList(hdfs, fileStatus, context);
            }
          }
        } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      } 
      
      
    }

    private void addFileToReadList(FileSystem hdfs, FileStatus fileStatus,
        TaskAttemptContext context) {
      
      
      filesToRead.add(fileStatus);
      
      totalBytesToRead += fileStatus.getLen();
      totalRows++;
      
      context.getCounter("ConfigurableInputFormat", "files.to.read").increment(1);
      context.getCounter("ConfigurableInputFormat", "files.bytes.to.read").increment(fileStatus.getLen());
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
    
      if (finishedRows  > -1) {
        
        totalBytesRead += filesToRead.get((int)finishedRows).getLen();
        context.getCounter("ConfigurableInputFormat", "files.finished.reading").increment(1);
        context.getCounter("ConfigurableInputFormat", "files.bytes.finished.reading").increment(filesToRead.get((int)finishedRows).getLen());
      } 
      
      if (finishedRows < totalRows -1) {
         
        finishedRows += 1;
        context.getCounter("ConfigurableInputFormat", "files.starting.to.read").increment(1);
        if (finishedRows < totalRows) {
          context.getCounter("ConfigurableInputFormat", "files.bytes.starting.to.reading").increment(filesToRead.get((int)finishedRows).getLen());
        }
        return true;
      } else {
        
        return false;
      }
    }

    LongWritable newKey = new LongWritable();
    
    @Override
    public LongWritable getCurrentKey() throws IOException,
        InterruptedException {
      newKey.set(finishedRows);
      return newKey;
    }

    Text newValue = new Text();
    
    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      newValue.set(filesToRead.get((int)finishedRows).getPath().toString());
      return newValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {

      System.out.println("GetProgress:" + (float)totalBytesRead/(float)totalBytesToRead);
      return (float)totalBytesRead/(float)totalBytesToRead;
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException,
      InterruptedException {

    int splits = context.getConfiguration().getInt(MAPRED_MAP_TASKS, 2);

    List<InputSplit> splitList = new ArrayList<InputSplit>();

    for (int i = 0; i < splits; i++) {
      splitList.add(new ConfigurableInputSplit());
    }

    return splitList;
  }

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    ConfigurableRecordReader reader = new ConfigurableRecordReader();
    reader.initialize((ConfigurableInputSplit) (split), context);

    return reader;
  }

  public static void setInputPath(Job job, String inputPath) {
    job.getConfiguration().set(INPUT_PATH, inputPath);
  }
  
  public static void setMapperNumber(Job job, int numberOfMappers) {

    job.getConfiguration().set(MAPRED_MAP_TASKS, Integer.toString(numberOfMappers));
  }

}
