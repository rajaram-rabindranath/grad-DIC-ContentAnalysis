package kmeans;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> 
{
	private static final transient Logger LOG = LoggerFactory.getLogger(KMeansMapper.class);
	FeatureSpace dataPoint = null;
	private Properties properties = new Properties();
	
	//
	double minDist = 0.0f;
	double currDist = 0.0f;
	int memberOf = 0;
	
	private int friend_field_ID = 0;
	private int followers_field_ID = 0;
	private int status_field_ID = 0;
	private int screenName_field_ID = 0;

	ArrayList<Cluster> clusterList = new ArrayList<Cluster>();
	
	protected void setup(Context context)
	{
		// field ids
		try
		{
			String stopwordCacheName = new Path("/conf/headers_twitterData.properties").getName();
			File file = new File(stopwordCacheName);
			Path cachePath = new Path(file.getAbsolutePath());
			if (cachePath.getName().equals(stopwordCacheName))
			{
				properties.load(new FileInputStream(cachePath.toString()));
			}
			
			/**
			SCREEN_NAME 1
			FRIENDS_COUNT 9
			FOLLOWERS_COUNT 10
			FAVOURITES_COUNT 11
			STATUSES_COUNT 12
			**/
			friend_field_ID = Integer.valueOf(properties.getProperty("FRIENDS_COUNT")) - 1;
			followers_field_ID = Integer.valueOf(properties.getProperty("FOLLOWERS_COUNT")) - 1;
			status_field_ID = Integer.valueOf(properties.getProperty("STATUSES_COUNT")) - 1;
			screenName_field_ID = Integer.valueOf(properties.getProperty("SCREEN_NAME")) -1;
		} 
		catch (IOException e) 
		{
			LOG.error(e.getStackTrace().toString());
		}
		
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
                clusterList.add(clust);
            }
            br.close();
		}
		catch(IOException iex)
		{
			iex.printStackTrace();
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
		// the mapper is sent n-lines at a time -- not the whole document
		String doc = value.toString();
		// if suppose the mapper was given N lines --- we shall split it this way
		String[] lines = doc.split("\n");
		
		for(String line : lines) 
		{
			
			String[] contents = line.split("\t");
			dataPoint = new FeatureSpace();
		
			dataPoint.friendsCount = Integer.valueOf(contents[friend_field_ID]);
			dataPoint.followersCount = Integer.valueOf(contents[followers_field_ID]);
			dataPoint.statusCount = Integer.valueOf(contents[status_field_ID]);
			dataPoint.screenName = contents[screenName_field_ID];
			
			if(dataPoint.friendsCount < 0) dataPoint.friendsCount = 0;
			if(dataPoint.followersCount < 0) dataPoint.followersCount = 0;
			
			// re init
			minDist = 0.0f;
			currDist =0.0f;
			memberOf=0;
			
			// compute membership of each datapoint
			for(int i=0;i<clusterList.size();i++)
			{
				currDist=euclid_dist(clusterList.get(i), dataPoint);
				if(i==0) // first iteration
				{
					minDist = currDist;
					memberOf = i;
				}
				else
				{
					minDist = (currDist < minDist) ? currDist : minDist;
					if(minDist == currDist) memberOf = i;
				}
			}
			dataPoint.distFromCentroid = minDist;
			String dataPointFlatten = dataPoint.screenName+";"+dataPoint.friendsCount+";"+dataPoint.followersCount+";"+dataPoint.statusCount+";"+dataPoint.distFromCentroid;
			Text c = new Text(dataPointFlatten);
			context.write(new IntWritable(memberOf),c);
		}
	}
	
	public double euclid_dist(Cluster centroid, FeatureSpace dataPoint)
	{
		double x = (centroid.followersCount.get() - dataPoint.followersCount);
		double y = (centroid.followersCount.get() - dataPoint.followersCount);
		double z = (centroid.statusCount.get() - dataPoint.statusCount);
		double dist = Math.sqrt(x*x+ y*y + z*z);
		return dist;
	}
}