package org.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * 使用mapreduce进行简单排序
 */
public class GeneralSort {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, new Text(""));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,	 Context context)
				throws IOException, InterruptedException {			
			context.write(key, new Text(""));			
		}
	}
	
	public static void main(String[] args) throws Exception {
		runJob();
	}
	
	public static void runJob() throws Exception{
		String input = "file:///home/ym/ytmp/data/n1";
		String output = "file:///home/ym/ytmp/output/n1s";
		
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "GeneralSort");
		job.setJarByClass(GeneralSort.class);
		
		Path inPath = new Path(input);
		Path outputPath = new Path(output);
		FileInputFormat.setInputPaths(job, inPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);		
		
		job.setOutputKeyClass(Text.class);		// 键在写入输出文件时以Text的形式
		job.setOutputValueClass(Text.class);	// 值在写入输出文件时以Text的形式
		job.waitForCompletion(true);
	}
}
