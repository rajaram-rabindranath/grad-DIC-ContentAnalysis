package relative_tagcoocc.stripes;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesRelativeHashTagCoOccCombiner extends Reducer<Text, MapWritable, Text, MapWritable> {
	private MapWritable result;

	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		result = new MapWritable();
		Iterator<MapWritable> iter = values.iterator();
		while (iter.hasNext()) {
			for (Entry<Writable, Writable> e : iter.next().entrySet()) {
				Writable inner_key = e.getKey();
				IntWritable fromCount = (IntWritable) e.getValue();
				if (result.containsKey(inner_key)) {
					IntWritable count = (IntWritable) result.get(inner_key);
					count.set(count.get() + fromCount.get());
				} else
					result.put(inner_key, fromCount);

			}
		}
		context.write(key, result);
	}

}
