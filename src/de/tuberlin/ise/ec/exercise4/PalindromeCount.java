package de.tuberlin.ise.ec.exercise4;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.hamcrest.core.IsNull;

/**
 * Count the palindromes with at least 5 letters in the text corpus.
 * 
 * @author markus klems
 * 
 */
public class PalindromeCount {

	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			// TODO count palindromes here
			while(tokenizer.hasMoreTokens()){
				word.set(tokenizer.nextToken());
				boolean palindrome = isPalindromeWithAtLeast5Letters(word);
				if(palindrome){
					output.collect(word, one);
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}

	private static boolean isPalindromeWithAtLeast5Letters(Text word) {
		// TODO check if the word has at least 5 letters
		// TODO check if the word is a palindrome
		int length = word.getLength();
		boolean palindrome = false;
		if(length >= 5){
			int idxFirst = 0;
			int idxLast = length - 1;
			while(idxLast > idxFirst){
				if(word.charAt(idxFirst) == word.charAt(idxLast)){
					palindrome = true;
					idxFirst++;
					idxLast--;
				}else{
					return false;
				}
			}
			if(isNumber(word)){
				return false;
			}
		}
		return palindrome;
	}
	
	private static boolean isNumber(Text word){
		try{
			Double.parseDouble(word.toString());
		}catch(NumberFormatException ex){
			return false;
		}
		return true;
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName("wordcount");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		JobClient.runJob(conf);
	}

}
