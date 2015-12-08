package edu.umich.cse.eecs485;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.classifier.bayes.XmlInputFormat;
import java.util.HashSet;
import nu.xom.*;
import java.util.*;
import java.io.*;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InvertedIndexPartThree
{

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text textValue = new Text();
		private final static LongWritable one = new LongWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);


			textValue.set(tokenizer.nextToken());
			String word = textValue.toString();
			textValue.set(tokenizer.nextToken());
			String theVal =	textValue.toString();

			context.write(new Text(word), new Text(theVal));

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

				int count = 0;

			String wholeVal;
			String pagid;
			String tfidf;

			StringBuffer otherBuffer = new StringBuffer(" ");


			for (Text value : values) {
				wholeVal = value.toString();
				pagid =	wholeVal.substring(0, wholeVal.indexOf(":") );
				tfidf = wholeVal.substring(wholeVal.indexOf(":") + 1, wholeVal.length());
				otherBuffer.append(" ");
				Double firstTemp = Double.parseDouble(pagid);
				int tempPage = firstTemp.intValue();
				otherBuffer.append(tempPage);
				otherBuffer.append(":");
				otherBuffer.append(tfidf);
				count++;
			}

			StringBuffer sBuffer = new StringBuffer("");
			sBuffer.append(" ");
			sBuffer.append(count);
			sBuffer.append(" ");
			sBuffer.append(otherBuffer.toString());
	
			context.write(key, new Text(sBuffer.toString()));
			
		
		}
	}


	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		Job job = new Job(conf);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
