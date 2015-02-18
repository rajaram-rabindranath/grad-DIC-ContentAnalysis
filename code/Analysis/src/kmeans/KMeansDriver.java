package kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansDriver
{
	private static final transient Logger LOG = LoggerFactory.getLogger(KMeansDriver.class);
	private static Properties properties = new Properties();
	
	
	static enum Kmeans_iterations 
	{
		moreIterations;
	}
	
	public static void main(String args[])
	{
		
		if(args.length < 1)
		{
			System.out.println("Well we need atleast 1 argument i.e the number of clusters we need to find");
			return;
		}
		
		// getting commandline args and using them
		int k = Integer.valueOf(args[0]);
		int reducetasks = 1; // default value assigned
		Cluster[] centroids = new Cluster[k];
		
		
		System.out.println("The number of centroids = "+k+":"+centroids.length);
		
		

		Path centroid_path = new Path("/conf/centroids");
			
		
		/**
		 * randomly create centroids
		 * and have it written to a file
		 * and have the mappers access the file
		 */
		try
		{
			
			FileSystem fs = FileSystem.get(new Configuration());
	        BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(centroid_path,true)));
	        String centroid_line ="";
			
	        // seeding the k centroids
			for(int i=0;i<k;i++)
			{
				/**
				 * making assumptions:
				 * 1. followers -- could range till 10000 
				 * 2. friends -- could range till 100s
				 * 3. statuses -- could range from 1000s
				 */
				centroids[i] = new Cluster();
				centroids[i].followersCount = new DoubleWritable(Math.random() * 10000 + 1); 
				centroids[i].friendsCount = new DoubleWritable(Math.random() * 10000 + 1);
				centroids[i].statusCount = new DoubleWritable(Math.random() * 1000 + 1);
				centroid_line = i+":"+centroids[i].friendsCount+":"+centroids[i].followersCount+
						":"+centroids[i].statusCount+":"+":";
				System.out.println(centroid_line);
		        bw.write(centroid_line+"\n");
		        centroid_line="";
		    }
			bw.close();
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}

		
		
		Configuration conf = new Configuration();
		conf.set("centroid.path", "/conf/centroids");
		LOG.info("HDFS Root Path: {}", conf.get("fs.defaultFS"));
		LOG.info("MR Framework: {}", conf.get("mapreduce.framework.name"));
		
		// start map reduce
		//boolean notConverged = true;
		long result =1;
		int iterationCount = 0;
		while(result > 0) 
		{
			

			System.out.println("============================================================");
			System.out.println("=======================      iteration::"+iterationCount+"        =======================");
			System.out.println("============================================================");

			
			try
			{
				properties.load(KMeansDriver.class.getResourceAsStream("/conf/configuration.properties"));
				LOG.info(properties.getProperty("output_path"));
				deleteFolder(conf,properties.getProperty("output_path"));
			}
			catch(IOException iex)
			{
				iex.printStackTrace();
				System.out.println("IOE exception encountered");
			}
			
			
			try
			{
				// set job details
				Job job = Job.getInstance(conf);
				cacheHeaders(job); // what does this do
				job.setNumReduceTasks(reducetasks);
				job.setJarByClass(KMeansDriver.class);
				job.setMapperClass(KMeansMapper.class);
				job.setReducerClass(KMeansReducer.class);
				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(Text.class);
				
				// location of input and where to output
				FileInputFormat.addInputPath(job, new Path(properties.getProperty("input_path")));
				FileOutputFormat.setOutputPath(job, new Path(properties.getProperty("output_path")));
				
				job.waitForCompletion(true);
				
				Counters jobCntrs = job.getCounters();
				result= jobCntrs.findCounter(Kmeans_iterations.moreIterations).getValue();
				
				System.out.println("============================================================");
				System.out.println("=================iterationEND::"+iterationCount+"=============");
				System.out.println("============================================================");
				iterationCount++;
			}
			catch(IOException iex)
			{
				iex.printStackTrace();
			}
			catch(InterruptedException inex)
			{
				inex.printStackTrace();
			}
			catch(ClassNotFoundException cnfe)
			{
				cnfe.printStackTrace();
			}
		}// end of while
		try
		{
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(centroid_path)));
			BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.append(centroid_path)));
			
			String line = null;
			String[] tokens = null;
			double clst_overall = 0.0f;
			
			while((line=br.readLine())!=null)
			{
				tokens = line.split(":");
				clst_overall+=Double.valueOf(tokens[4]);
			}
			
	        clst_overall = clst_overall/k;
	        bw.write("the cluster quality::"+clst_overall+"\n");
	        bw.write("The number of iterations::"+iterationCount+"\n");
	        bw.flush();
	        bw.close();
	        br.close();
		}
		catch(IOException ioex)
		{
			ioex.printStackTrace();
		}
	}
	
	
	private static void deleteFolder(Configuration conf, String folderPath ) throws IOException 
	{
		LOG.info("Called delete");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(folderPath);
		if(fs.exists(path))
		{
			fs.delete(path,true);
		}
	}
	
	private static void cacheHeaders(Job job) throws IOException
	{
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path hdfsPath = new Path(properties.getProperty("headers_out_path"));

		// upload the file to hdfs. Overwrite any existing copy.
		fs.copyFromLocalFile(false, true,new Path(properties.getProperty("headers_in_path")), hdfsPath);

		try
		{
			job.addCacheFile(new URI(properties.getProperty("headers_out_path")));
		}
		catch (URISyntaxException e) 
		{
			e.printStackTrace();
		}
	}
}
