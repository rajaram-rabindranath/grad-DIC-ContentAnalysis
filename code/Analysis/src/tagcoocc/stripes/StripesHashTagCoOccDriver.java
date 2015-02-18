package tagcoocc.stripes;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StripesHashTagCoOccDriver {

	private static final transient Logger LOG = LoggerFactory.getLogger(StripesHashTagCoOccDriver.class);

	private static Properties properties = new Properties();

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		properties.load(StripesHashTagCoOccDriver.class
				.getResourceAsStream("/conf/configuration.properties"));
		LOG.info(properties.getProperty("output_path"));
		
		int tasks = 5;
		try{
			tasks = Integer.parseInt(args[0]);
		}
		catch(NumberFormatException e){
			
		}
		catch(ArrayIndexOutOfBoundsException e){
			
		}
		/* FileOutputFormat wants to create the output directory itself.
		 * If it exists, delete it:
		 */
		deleteFolder(conf,properties.getProperty("output_path"));
		Job job = Job.getInstance(conf);
		cacheHeaders(job);
		job.addCacheFile(new URI("/conf/headers_twitterData.properties"));
		job.setNumReduceTasks(tasks);
		job.setJarByClass(StripesHashTagCoOccDriver.class);
		job.setMapperClass(StripesHashTagCoOccMapper.class);
//		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(StripesHashTagCoOccReducer.class);
		
		// Set the outputs for the Map
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(MapWritable.class);

        // Set the outputs for the Job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(properties.getProperty("input_path")));
		FileOutputFormat.setOutputPath(job, new Path(properties.getProperty("output_path")));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	/**
	 * Delete a folder on the HDFS. This is an example of how to interact
	 * with the HDFS using the Java API. You can also interact with it
	 * on the command line, using: hdfs dfs -rm -r /path/to/delete
	 * 
	 * @param conf a Hadoop Configuration object
	 * @param folderPath folder to delete
	 * @throws IOException
	 */
	private static void deleteFolder(Configuration conf, String folderPath ) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path)) {
			fs.delete(path,true);
		}
	}
	
	private static void cacheHeaders(Job job) throws IOException {
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(properties.getProperty("headers_out_path"));

		// upload the file to hdfs. Overwrite any existing copy.
		fs.copyFromLocalFile(false, true,
				new Path(properties.getProperty("headers_in_path")), hdfsPath);

		try {
			job.addCacheFile(new URI(properties.getProperty("headers_out_path")));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
}