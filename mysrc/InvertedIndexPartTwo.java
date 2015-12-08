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

public class InvertedIndexPartTwo
{

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text textValue = new Text();
		private final static LongWritable one = new LongWritable(1);

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();

			StringTokenizer tokenizer = new StringTokenizer(line);

			textValue.set(tokenizer.nextToken());
			double pagid =	Double.parseDouble(textValue.toString().substring(0, textValue.find(":") ) );
			String theWord = textValue.toString().substring(textValue.find(":") + 1, textValue.getLength());
			textValue.set(tokenizer.nextToken());
			double tfIk = Double.parseDouble( textValue.toString().substring(0, textValue.find(":")));
			double dF = Double.parseDouble(textValue.toString().substring(textValue.find(":") + 1, textValue.getLength()));

			double idf = Math.log10( 2 /  dF);

			double tfIdf =  tfIk * idf;

			StringBuffer sBuffer = new StringBuffer(theWord);
			sBuffer.append(":");
			sBuffer.append(Double.toString(tfIdf));


			context.write(new Text(Double.toString(pagid)), new Text(sBuffer.toString()));

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			HashSet<String> set = new HashSet<String>();

			double sum =0.0;

			for (Text value : values) {
				String wholeVal = value.toString();
				System.out.println(wholeVal);
				double temp =Double.parseDouble(wholeVal.toString().substring(wholeVal.indexOf(":") + 1, wholeVal.length()));
				sum += (temp * temp);
				set.add(wholeVal);
			}

			System.out.println("===================");

			sum = Math.sqrt(sum);

			Iterator<String> itr = set.iterator();

			while (itr.hasNext()) {
				String textValue = itr.next();
				String theWord =textValue.toString().substring(0, textValue.indexOf(":") );
				double tfidf = Double.parseDouble(textValue.toString().substring(textValue.indexOf(":") + 1, textValue.length()));
				double finalTfIdf = tfidf / sum;

				StringBuffer sBuffer = new StringBuffer(key.toString());
				sBuffer.append(":");
				sBuffer.append(Double.toString(finalTfIdf));

				context.write(new Text(theWord), new Text(sBuffer.toString()));
			}

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
