package shortestPath;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShortestPathDriver {

	private static final transient Logger LOG = LoggerFactory.getLogger(ShortestPathDriver.class);

	private static Properties properties = new Properties();

	static enum Iterations {
		numIter
	}

	public static void main(String[] args) throws Exception {
		int tasks = 1;
		int count = 0;
		long numIterLeft = 1;

		Configuration conf = new Configuration();
		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		properties.load(ShortestPathDriver.class.getResourceAsStream("/conf/configuration.properties"));
		LOG.info(properties.getProperty("output_path"));

		try {
			tasks = Integer.parseInt(args[0]);
		} catch (NumberFormatException e) {

		} catch (ArrayIndexOutOfBoundsException e) {

		}
		deleteFolder(conf, properties.getProperty("output_path"));
		
		while (numIterLeft > 0) {
			Job job = Job.getInstance(conf);
			cacheHeaders(job);
			job.setNumReduceTasks(tasks);
			job.setJarByClass(ShortestPathDriver.class);
			job.setMapperClass(ShortestPathMapper.class);
			job.setReducerClass(ShortestPathReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			String inputPath = null;
			String outputPath = null;

			if (count == 0) {
				inputPath = properties.getProperty("input_path");
			} else
				inputPath = properties.getProperty("graph_output_path") + "_" + count;
			outputPath = properties.getProperty("graph_output_path") + "_" + (count + 1);

			FileInputFormat.addInputPath(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath));

			job.waitForCompletion(true);

			Counters counters = job.getCounters();
			numIterLeft = counters.findCounter(Iterations.numIter).getValue();
			count++;
		}
	}

	private static void deleteFolder(Configuration conf, String folderPath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if (fs.exists(path)) {
			fs.delete(path, true);
		}
	}

	private static void cacheHeaders(Job job) throws IOException {
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(properties.getProperty("headers_out_path"));

		fs.copyFromLocalFile(false, true, new Path(properties.getProperty("headers_in_path")), hdfsPath);

		try {
			job.addCacheFile(new URI(properties.getProperty("headers_out_path")));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
}