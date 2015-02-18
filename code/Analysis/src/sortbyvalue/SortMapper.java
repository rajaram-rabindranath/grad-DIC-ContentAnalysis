package sortbyvalue;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SortMapper extends Mapper<Object, Text, LongWritable, Text> {
	private static final transient Logger LOG = LoggerFactory.getLogger(SortMapper.class);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] contents = line.split("\t");
		LOG.debug("Size: " + contents.length + "Contents: " + line);
		if (contents.length == 2) {
			String innerkey = contents[0];
			String innerval = contents[1];
			word.set(innerkey);
			LongWritable intKey = new LongWritable();
			intKey.set(Integer.parseInt(innerval));
			context.write(intKey, word);
		}
	}

}