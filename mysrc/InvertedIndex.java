package edu.umich.cse.eecs485;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.mahout.classifier.bayes.XmlInputFormat;
import java.util.HashSet;
import java.util.*;
import nu.xom.*;
import java.io.*;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InvertedIndex
{

	public static Vector<String> stop_words = new Vector<String>();

	public static void readStopWords(){
		String line = "";
	 	try{
	 		FileReader file = new FileReader("stop_words.txt");
	 		BufferedReader bufferedReader = new BufferedReader(file);
	 		line = bufferedReader.readLine();
	 		while (line != null)
			{
				stop_words.add(line);
				line = bufferedReader.readLine();
	  		}
	  		bufferedReader.close(); 
		}
 		catch(Exception e)
 		{
			System.err.println("Error reading file stop_words.txt");
 		}
 		if (stop_words.isEmpty())
 		{
			System.err.println("Error with file reader on stop_words.txt");
 			System.exit(1);
 		}
 		Collections.sort(stop_words);
	}


	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String strId = "";
			String strBody = "";

			// Parse the xml and read data (page id and article body)
			// Using XOM library
			Builder builder = new Builder();

			try {
//				System.out.println(value.toString());
				Document doc = builder.build(value.toString(), null);

				Nodes nodeId = doc.query("//eecs485_article_id");
				strId = nodeId.get(0).getChild(0).getValue();
				
				Nodes nodeBody = doc.query("//eecs485_article_body");
				strBody = nodeBody.get(0).getChild(0).getValue();
			}
			// indicates a well-formedness error
			catch (ParsingException ex) { 
				System.out.println("Not well-formed.");
				System.out.println(ex.getMessage());
			}  
			catch (IOException ex) {
				System.out.println("io exception");
			}
			
			// Tokenize document body
			Pattern pattern = Pattern.compile("\\w+");
			Matcher matcher = pattern.matcher(strBody);
			
			while (matcher.find()) {
				// Write the parsed token
				if(!stop_words.contains(matcher.group().toLowerCase()))
				{
					context.write(new Text(matcher.group().toLowerCase()), new LongWritable(Integer.valueOf(strId)));
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, LongWritable, Text, Text> {
		
		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			
			// use a set to keep a set of unique doc IDs
			ArrayList<String> allPagesForWord = new ArrayList<String>();
			
			for (LongWritable value : values) {
				String pagid = String.valueOf(value.get());
				allPagesForWord.add(pagid);
			}

			Collections.sort(allPagesForWord);
			Set<String> df = new HashSet<String>(allPagesForWord);

			int count = 1; 
			int dfSize = df.size();

			for(int i  = 0; i < allPagesForWord.size(); i++)
			{
				if(i + 1 == allPagesForWord.size() || !allPagesForWord.get(i).equals(allPagesForWord.get(i + 1)))
				{

					StringBuffer sBuffer = new StringBuffer("");
					StringBuffer tfDfCount = new StringBuffer("");
					
					sBuffer.append(allPagesForWord.get(i));
					sBuffer.append(":");
					sBuffer.append(key);
					tfDfCount.append(count);
					tfDfCount.append(":");
					tfDfCount.append(dfSize);

					context.write(new Text(sBuffer.toString()), new Text(tfDfCount.toString()));
					count = 1; 
				
				}
				else count++;
			}
		}
	}


	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();

		conf.set("xmlinput.start", "<eecs485_article>");
		conf.set("xmlinput.end", "</eecs485_article>");

		InvertedIndex.readStopWords();

		Job job = new Job(conf, "XmlParser");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(XmlInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
