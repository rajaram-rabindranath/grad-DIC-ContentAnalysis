package kmeans;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Cluster implements Writable 
{
	DoubleWritable friendsCount;
	DoubleWritable followersCount;
	DoubleWritable statusCount;
	DoubleWritable memberCount;
	IntWritable centroidID;
	DoubleWritable clusterQuality;
	
	public Cluster()
	{
		friendsCount = new DoubleWritable();
		followersCount = new DoubleWritable();
		statusCount = new DoubleWritable(); 
		memberCount =new DoubleWritable();
		centroidID =  new IntWritable();
		clusterQuality=new DoubleWritable();
	}
	
	public Cluster(IntWritable centroidID,DoubleWritable friendsCount,DoubleWritable followersCount,DoubleWritable statusCount)
	{
		this.centroidID =  centroidID;
		this.followersCount = followersCount;
		this.friendsCount = friendsCount;
		this.statusCount = statusCount;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException
	{
		friendsCount.readFields(arg0);
		followersCount.readFields(arg0);
		statusCount.readFields(arg0);
		memberCount.readFields(arg0);
		centroidID.readFields(arg0);
	}
	
	@Override
	public void write(DataOutput arg0) throws IOException 
	{
		friendsCount.write(arg0);
		followersCount.write(arg0);
		statusCount.write(arg0);
		memberCount.write(arg0);
		centroidID.write(arg0);
	}
}