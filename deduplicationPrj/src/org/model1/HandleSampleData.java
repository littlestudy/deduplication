package org.model1;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HandleSampleData  extends Configured implements Tool{
	private static String[] targetFileds = new String [] {
			"deviceOsVersion"
			, "deviceResolution"
			, "ip"
			, "logCity"
			, "persistedTime"
			, "occurTime"
			, "sessionUuid" 
			, "eventId"
			, "sessionStep"				
			};
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] fileds = value.toString().split(",");
			StringBuilder sbKey = new StringBuilder();
			StringBuilder sbValue = new StringBuilder();
			//System.out.println(fileds.length);
			for (int i = 0; i < 5; i++)
				sbKey.append("," + fileds[i].trim());
		
			for (int i = 5; i < fileds.length; i++)
				sbValue.append("," + fileds[i].trim());
			
			// System.out.println(sbKey.toString().substring(1) + " -- " + sbValue.toString().substring(1));
			context.write(new Text(sbKey.toString().substring(1)), new Text(sbValue.toString().substring(1)));
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,	 Context context)
				throws IOException, InterruptedException {			
			StringBuilder sb = new StringBuilder();
			for (Text val : values){
				sb.append(",{" + val.toString().trim() +"}");
			}
			context.write(key, new Text("{" + sb.toString().substring(1) + "}"));
		}
	}
	
	
	public static class MapClass2 extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String [] fileds = value.toString().split(",");
			StringBuilder sbKey = new StringBuilder();
			StringBuilder sbValue = new StringBuilder();
			//System.out.println(fileds.length);
			for (int i = 0; i < 2; i++)
				sbKey.append("," + fileds[i].trim());
		
			for (int i = 2; i < fileds.length; i++)
				sbValue.append("," + fileds[i].trim());
			
			// System.out.println(sbKey.toString().substring(1) + " -- " + sbValue.toString().substring(1));
			context.write(new Text(sbKey.toString().substring(1)), new Text(sbValue.toString().substring(1)));
		}
	}

	public static class Reduce2 extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values,	 Context context)
				throws IOException, InterruptedException {			
			StringBuilder sb = new StringBuilder();
			for (Text val : values){
				sb.append(",{" + val.toString().trim() +"}");
			}
			context.write(key, new Text("{" + sb.toString().substring(1) + "}"));
		}
	}
	/*
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		//String job1_in = "file:///home/ym/ytmp/new/testdata";
		String job1_out = "file:///home/ym/ytmp/output/6";
		String job2_out = "file:///home/ym/ytmp/output/7";
		
		Job job = new Job(conf, "MyJob");
		job.setJarByClass(HandleSampleData.class);
		
		Path in = new Path(job1_out);
		Path out = new Path(job2_out);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		
		job.setOutputFormatClass(HandleSampleDataOutput.class);
		job.setOutputKeyClass(Text.class);		// 键在写入输出文件时以Text的形式
		job.setOutputValueClass(Text.class);	// 值在写入输出文件时以Text的形式
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		return 0;
	}
	*/
	public static void main(String[] args) throws Exception {	
		int res = ToolRunner.run(new Configuration(), new HandleSampleData(), args);
		System.exit(res);
	}
	
	
	@Override
	public int run(String[] arg0) throws Exception {
		String job1_in = "file:///home/ym/ytmp/data/testdata";
		String job1_out = "file:///home/ym/ytmp/output/9";
		String job2_out = "file:///home/ym/ytmp/output/10";
		
		Configuration conf = new Configuration();
		
		Job job1 = new Job(conf, "MyJob1");
		job1.setJarByClass(HandleSampleData.class);
		
		Path in1 = new Path(job1_in);
		Path out1 = new Path(job1_out);
		FileInputFormat.setInputPaths(job1, in1);
		FileOutputFormat.setOutputPath(job1, out1);
		
		job1.setMapperClass(MapClass.class);
		job1.setReducerClass(Reduce.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		
		job1.setOutputFormatClass(HandleSampleDataOutput.class);
		job1.setOutputKeyClass(Text.class);		// 键在写入输出文件时以Text的形式
		job1.setOutputValueClass(Text.class);	// 值在写入输出文件时以Text的形式
		
		Job job2 = new Job(conf, "MyJob2");
		job2.setJarByClass(HandleSampleData.class);
		
		Path in2 = new Path(job1_out);
		Path out2 = new Path(job2_out);
		FileInputFormat.setInputPaths(job2, in2);
		FileOutputFormat.setOutputPath(job2, out2);
		
		job2.setMapperClass(MapClass2.class);
		job2.setReducerClass(Reduce2.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		
		job2.setOutputFormatClass(HandleSampleDataOutput.class);
		job2.setOutputKeyClass(Text.class);		// 键在写入输出文件时以Text的形式
		job2.setOutputValueClass(Text.class);	// 值在写入输出文件时以Text的形式
		

		ControlledJob cJob1 = new ControlledJob(conf);
		cJob1.setJob(job1);
		
		List<ControlledJob> dependingJobs = new ArrayList<ControlledJob>();
		dependingJobs.add(cJob1);
		
		ControlledJob cJob2 = new ControlledJob(job2, dependingJobs);
		
		JobControl jc = new JobControl("test");
		jc.addJob(cJob1);
		jc.addJob(cJob2);		
		
		new Thread(jc).start();
		
		while (true){
			if (jc.allFinished()){
				System.out.println("All tasks are finished!");
				break;
			}
		}
		
		return 0;
	}
	
}

























