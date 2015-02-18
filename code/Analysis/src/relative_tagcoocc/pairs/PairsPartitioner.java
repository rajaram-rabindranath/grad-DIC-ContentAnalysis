package relative_tagcoocc.pairs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PairsPartitioner extends Partitioner<Text, IntWritable> {

	@Override
	public int getPartition(Text key, IntWritable value, int numReduceTasks) {

		String[] name = key.toString().split(",");
		String primaryKey = name[0];
		Text text = new Text(primaryKey);

		if (numReduceTasks == 0)
			return 0;
		else
			return Math.abs(text.hashCode() % numReduceTasks);

	}
}
