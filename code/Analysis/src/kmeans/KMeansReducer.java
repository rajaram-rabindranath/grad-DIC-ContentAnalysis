package kmeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

import kmeans.KMeansDriver.Kmeans_iterations;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> 
{
	ArrayList<Cluster> initClusters =  new ArrayList<Cluster>();
	ArrayList<Cluster> myClusters = new ArrayList<Cluster>();
	
	private static final transient Logger LOG = LoggerFactory.getLogger(KMeansReducer.class);
	
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		int memberCount = 0;
		String tokens[] = null;
		
		double cent_friends=0.0f;
		double cent_followers=0.0f;
		double cent_statuses=0.0f;
		double clusterQuality = 0.0f;
		
		for (Text val : values) 
		{
			// split value ;
			memberCount++; // members of a cluster
			// tokenize the values
			tokens= val.toString().split(";");
			cent_friends += Integer.valueOf(tokens[1]);
			cent_followers += Integer.valueOf(tokens[2]);
			cent_statuses += Integer.valueOf(tokens[3]);
			context.write(key,val);
			clusterQuality +=Double.valueOf(tokens[4]); 
		}
		
		/*
		 * create cluster object
		 */
		Cluster cluster = new Cluster();
		cluster.centroidID= key;
		cluster.followersCount.set(cent_followers/memberCount);
		cluster.friendsCount.set(cent_friends/memberCount);
		cluster.statusCount.set(cent_statuses/memberCount);
		cluster.clusterQuality.set(clusterQuality/memberCount);
		cluster.memberCount.set(memberCount);
		
		myClusters.add(cluster);
	}
	
	protected void cleanup(Context context) throws IOException,InterruptedException 
	{
	    super.cleanup(context);
	    
	    // find the location of file tht holds the centroids
	    Configuration conf = context.getConfiguration();
	    Path centroid_path = new Path("/conf/centroids");
	    FileSystem fs = FileSystem.get(conf);
	    fs.delete(centroid_path, true);
	  
	    BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(centroid_path,true)));
        String centroid_line ="";
        Cluster clst = null;
	    for(int i=0;i<myClusters.size();i++)
	    {
	    	clst = myClusters.get(i);
	    	centroid_line = i+":"+clst.friendsCount+":"+clst.followersCount+":";
	    	centroid_line += clst.statusCount+":"+clst.clusterQuality+":"+clst.memberCount;
	    	bw.write(centroid_line+"\n");
	    }
	    bw.close();
	    
	    //have converged
		for(int i=0;i<myClusters.size();i++)
		{
			if(!hasConverged(myClusters.get(i),initClusters.get(i)))
			{
				context.getCounter(Kmeans_iterations.moreIterations).increment(1L);
				break;
			}
		}
	  }

	
	protected void setup(Context context)
	{
		// learn the initial centroid
		try
		{
			Path centroid_path = new Path("/conf/centroids");
			FileSystem fs = FileSystem.get(new Configuration());
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(centroid_path)));
			String centroidData = null;
			String[] tokens = null;
		    Cluster clust = null;
		    while ((centroidData=br.readLine())!= null)
		    {
		        LOG.info(centroidData);
		        tokens = centroidData.split(":") ;
		        clust = new Cluster(new IntWritable(Integer.valueOf(tokens[0])),new DoubleWritable(Double.valueOf(tokens[1])),new DoubleWritable(Double.valueOf(tokens[2])),new DoubleWritable(Double.valueOf(tokens[3]))) ;
		        initClusters.add(clust);
		    }
		    
		    br.close();
		}
		catch(IOException iex)
		{
			iex.printStackTrace();
		}
	}
	
	private boolean hasConverged(Cluster A,Cluster B)
	{
		// has the cluster centroid moved
		long delta_friends = Math.abs(Math.round(A.friendsCount.get() - B.friendsCount.get()));
		long delta_followers = Math.abs(Math.round(A.followersCount.get() - B.followersCount.get()));
		long delta_statuses = Math.abs(Math.round(A.statusCount.get() - B.statusCount.get()));
		
		long delat_totl = delta_friends+delta_followers+delta_statuses;
		/*
		System.out.println("friends_delta ="+(A.friendsCount.get() - B.friendsCount.get())+"::"+delta_friends);
		System.out.println("followers_delta"+(A.followersCount.get() - B.followersCount.get())+"::"+delta_followers);
		System.out.println("status_delta"+(A.statusCount.get() - B.statusCount.get())+"::"+delta_statuses);
		System.out.println("The delta total::"+delat_totl+"::"+new Long(15).compareTo(new Long(delat_totl)));*/
		if(new Long(0).compareTo(new Long(delat_totl))==0) 
			return true;
		else
			return false;
	}
}