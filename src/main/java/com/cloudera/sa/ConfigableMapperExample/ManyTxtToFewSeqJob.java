package com.cloudera.sa.ConfigableMapperExample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.cloudera.sa.ConfigableMapperExample.util.ConfigurableInputFormat;

public class ManyTxtToFewSeqJob {
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out
					.println("ManyTxtToFewSeqJob <inputPath> <outputPath> <# mappers> <compressionCodec>");
			System.out.println();
			System.out
					.println("Example: ManyTxtToFewSeqJob ./input ./output 20 snappy");
			return;
		}

		// Get values from args
		String inputPath = args[0];
		String outputPath = args[1];
		String numberOfMappers = args[2];
		String compressionCodec = args[3];

		// Create job
		Job job = new Job();
		job.setJobName("ManyTxtToFewSeqJob");

		job.getConfiguration().set("mapred.map.tasks", numberOfMappers);

		job.getConfiguration().set("custom.input.folder", inputPath);

		job.setJarByClass(ManyTxtToFewSeqJob.class);
		// Define input format and path
		job.setInputFormatClass(ConfigurableInputFormat.class);

		// Define output format and path
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
		if (compressionCodec.toLowerCase().equals("gzip")) {
			SequenceFileOutputFormat.setOutputCompressorClass(job,
					GzipCodec.class);
		} else if (compressionCodec.toLowerCase().equals("bzip2")) {
			SequenceFileOutputFormat.setOutputCompressorClass(job,
					BZip2Codec.class);
		} else {
			SequenceFileOutputFormat.setOutputCompressorClass(job,
					SnappyCodec.class);
		}

		SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));

		// Define the mapper and reducer
		job.setMapperClass(ConsalidatorMapper.class);
		// job.setReducerClass(Reducer.class);

		// Define the key and value format
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setNumReduceTasks(0);

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		hdfs.delete(new Path(outputPath), true);

		// Exit
		job.waitForCompletion(true);
	}

	public static class ConsalidatorMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		Text newKey = new Text();
		Text newValue = new Text();

		@Override
		public void setup(Context context) throws IOException {

			System.out.println("Setup");

			int numberOfMappers = context.getConfiguration().getInt(
					"mapred.map.tasks", -1);

			if (numberOfMappers == -1) {
				throw new RuntimeException();
			}

			String inputFolder = context.getConfiguration().get(
					"custom.input.folder");

			int taskId = context.getTaskAttemptID().getTaskID().getId();

			try {
				Configuration config = new Configuration();
				FileSystem hdfs = FileSystem.get(config);

				Path inputFolderPath = new Path(inputFolder);

				if (hdfs.isDirectory(inputFolderPath)) {
					FileStatus[] fileStatuses = hdfs
							.listStatus(inputFolderPath);

					for (FileStatus fileStatus : fileStatuses) {

						Path sourceFilePath = fileStatus.getPath();

						if (sourceFilePath.getName().hashCode()
								% numberOfMappers == taskId) {
							processSingleFile(hdfs, sourceFilePath, context);
						}
					}

				} else {
					// We have a single file so only the first mapper can work
					// on it
					if (taskId == 0) {
						processSingleFile(hdfs, inputFolderPath,
								context);
					}
				}

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		private void processSingleFile(FileSystem hdfs, Path sourceFilePath,
				Context context) throws IOException, InterruptedException {
			
			System.out.println("Reading:" + sourceFilePath);
			context.getCounter("Files", "NumberOfFiles").increment(1);
			FileStatus fileStatus = hdfs.getFileStatus(sourceFilePath);
			context.getCounter("Files", "NumberOfFileBytes").increment(fileStatus.getLen());
			context.getCounter("Files", "NumberOfBlocks").increment(fileStatus.getLen() / fileStatus.getBlockSize());
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(sourceFilePath)));
			
		    try {
		    	Text newValue = new Text();
		    	LongWritable longWritable = new LongWritable(0);
				String line = reader.readLine();
				
			    while ( line != null ) {
			    	newValue.set(line);
			    	context.write(longWritable, newValue);
			    	
			    	context.getCounter("Files", "BytesWriten").increment(line.length());
			    	context.getCounter("Files", "RecordsWriten").increment(1);
			    	
			    	line = reader.readLine();
			    }
		    }finally {
		    	reader.close();
		    }
		}
	}

}
