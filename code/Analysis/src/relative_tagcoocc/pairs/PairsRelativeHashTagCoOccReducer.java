package relative_tagcoocc.pairs;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PairsRelativeHashTagCoOccReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
	private DoubleWritable result = new DoubleWritable();
	private String word = "NULL";
	private double marginal;

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		String[] pair = key.toString().split(",");
		if (pair[1].equals("*")) {
			if (!word.equals(pair[0])) {
				word = pair[0];
				marginal = 0;
			}
			for (IntWritable val : values) {
				marginal += val.get();
			}
		} else {
			double sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum / marginal);
			context.write(key, result);
		}
	}
}
