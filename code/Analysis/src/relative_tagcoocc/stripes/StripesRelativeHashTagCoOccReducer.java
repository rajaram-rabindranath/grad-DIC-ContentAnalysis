package relative_tagcoocc.stripes;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

public class StripesRelativeHashTagCoOccReducer extends Reducer<Text, MapWritable, Text, Text> {
	private MapWritable result;
	private int marginal;

	public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
		result = new MapWritable();
		Iterator<MapWritable> iter = values.iterator();
		marginal = 0;
		while (iter.hasNext()) {
			for (Entry<Writable, Writable> e : iter.next().entrySet()) {
				Writable inner_key = e.getKey();
				IntWritable newCount = (IntWritable) e.getValue();
				if (inner_key.toString().equals("*"))
					marginal += newCount.get();
				else {
					if (result.containsKey(inner_key)) {
						IntWritable count = (IntWritable) result.get(inner_key);
						count.set(count.get() + newCount.get());
						result.put(inner_key, count);
					} else
						result.put(inner_key, newCount);
				}

			}
		}
		StringBuffer value = new StringBuffer();
		for (Entry<Writable, Writable> e : result.entrySet()) {
			Writable inner_key = e.getKey();
			IntWritable inner_value = (IntWritable) e.getValue();
			String appended = inner_key.toString() + ", " + (double) inner_value.get() / (double) marginal + "; ";
			value.append(appended);
		}
		context.write(key, new Text(value.toString()));
	}

}
