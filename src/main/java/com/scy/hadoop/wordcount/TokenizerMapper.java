package com.scy.hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends
		Mapper<IntWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(IntWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// StringTokenizer是java工具类，将字符串按照空格进行分割
		StringTokenizer itr = new StringTokenizer(value.toString());

		// 每次出现一个单词，单词次数加1
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one);
		}
	}
}
