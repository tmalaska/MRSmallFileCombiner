package com.cloudera.sa.ConfigableMapperExample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
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


		job.setJarByClass(ManyTxtToFewSeqJob.class);
		// Define input format and path
		job.setInputFormatClass(ConfigurableInputFormat.class);
		ConfigurableInputFormat.setInputPath(job, inputPath);
		ConfigurableInputFormat.setMapperNumber(job, Integer.parseInt(numberOfMappers));

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
		job.setOutputKeyClass(BytesWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setMapOutputKeyClass(BytesWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setNumReduceTasks(0);

		Configuration config = new Configuration();
		FileSystem hdfs = FileSystem.get(config);

		hdfs.delete(new Path(outputPath), true);

		// Exit
		job.waitForCompletion(true);
	}

	public static class ConsalidatorMapper extends
			Mapper<LongWritable, Text, BytesWritable, BytesWritable> {
		Text newKey = new Text();
		Text newValue = new Text();
		Configuration config;
		FileSystem hdfs;
		

		@Override
		public void setup(Context context) throws IOException {
      config = new Configuration();
      hdfs = FileSystem.get(config);
		}
		
		@Override
		public void cleanup(Context context) throws IOException {
		  //hdfs.close();
		}

		@Override
		public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException{
		  
		  processSingleFile(new Path(value.toString()), context);
		}
		
		private void processSingleFile(Path sourceFilePath,
				Context context) throws IOException, InterruptedException {
			
			context.getCounter("Files", "NumberOfFiles").increment(1);
			FileStatus fileStatus = hdfs.getFileStatus(sourceFilePath);
			context.getCounter("Files", "NumberOfFileBytes").increment(fileStatus.getLen());
			context.getCounter("Files", "NumberOfBlocks").increment(fileStatus.getLen() / fileStatus.getBlockSize());
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(hdfs.open(sourceFilePath)));
			
		    try {
		      BytesWritable byteValue = new BytesWritable();
		      BytesWritable byteWritable = new BytesWritable();
				String line = reader.readLine();
				
			    while ( line != null ) {
			      byteValue.set(line.getBytes(), 0, line.getBytes().length);
			    	context.write(byteWritable, byteValue);
			    	
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
