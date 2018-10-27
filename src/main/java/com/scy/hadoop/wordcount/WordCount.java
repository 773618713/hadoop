package com.scy.hadoop.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class WordCount {
	 public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			final String nameNodeUrl = "hdfs://182.254.247.119:9000";
			conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, nameNodeUrl);
	 
			//1 构建job对象，因为hadoop中可能会同时运行多个任务，每个任务都会有一个名字，以示区分
			final String jobName = "word count";
			Job job = Job.getInstance(conf, jobName);
			job.setJarByClass(WordCount.class);
			// 任务内容的输入路径
			FileInputFormat.addInputPath(job, new Path("user/root/input_wordcount/file1"));
			// 任务计算结果的而输出路径，如果输出目录已经存在，就删除
			final Path outputPath = new Path("/output_wordcount");
			FileSystem fileSystem = outputPath.getFileSystem(conf);
			if(fileSystem.exists(outputPath)){
				fileSystem.delete(outputPath, true);
			}
			FileOutputFormat.setOutputPath(job, outputPath);
			
			
			//2 设置Mapper
			job.setMapperClass(TokenizerMapper.class);
			//规约，后文会详细介绍
//			job.setCombinerClass(IntSumReducer.class);
			
			//3 设置Reducer
			//设置分区
			job.setReducerClass(IntSumReducer.class);
			//设置reducer任务数，默认为1
			job.setNumReduceTasks(1);
			//设置分区类
			job.setPartitionerClass(HashPartitioner.class);
			//设置输出的key与Value
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
	 
			// 提交任务并等待执行完成
			job.waitForCompletion(true);
		}
}
