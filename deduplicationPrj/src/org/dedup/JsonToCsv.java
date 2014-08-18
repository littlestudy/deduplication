package org.dedup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.dedup.ioformat.JsonInputFormat;

public class JsonToCsv {
	private static String[] targetFileds = new String [] {
		"appChannel"
		, "appKey"
		, "appVersion"
		, "city"
		, "dataType"
		, "deviceCarrier"
		, "deviceHashMac"
		, "deviceIMEI"
		, "deviceMacAddr"		
		, "deviceModel"
		, "deviceNetwork"
		, "deviceOs"
		, "deviceOsVersion"
		, "deviceResolution"
		, "deviceUdid"
		, "ip"
		, "isp"
		, "logCity"
		, "logProvince"
		, "occurTime"
		, "persistedTime"
		, "sessionUuid"
		, "userName"
		, "eventId"
		, "costTime"
		, "logSource"
		, "sessionStep"
		//, "attributes" //:{"toVer":"1.5.1","fromVer":"1.4.0"}}			
			};
	
	public static class MapClass extends Mapper<LongWritable, MapWritable, Text, Text>{
		@Override
		protected void map(LongWritable key, MapWritable value, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			System.out.println(value.toString());
			//for (java.util.Map.Entry<Writable, Writable> entry : value.entrySet()) {
			//	sb.append("," + entry.getValue().toString());				
		   // }
			for (int i = 0; i < targetFileds.length; i++){
				sb.append("," + value.get(new Text(targetFileds[i])).toString());	
			}
			context.write(new Text(sb.toString().substring(1)), new Text (""));
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

	public static int runJob(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, "MyJob");
		job.setJarByClass(JsonToCsv.class);
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(MapClass.class);
		//job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(JsonInputFormat.class);		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);		// 键在写入输出文件时以Text的形式
		job.setOutputValueClass(Text.class);	// 值在写入输出文件时以Text的形式
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		String input = "/home/ym/ytmp/data/1/0701.part.1";
		String output = "/home/ym/ytmp/output/8";
		runJob(new String[]{input, output});
	}
}

























