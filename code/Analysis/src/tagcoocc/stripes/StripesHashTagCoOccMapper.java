package tagcoocc.stripes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StripesHashTagCoOccMapper extends Mapper<Object, Text, Text, MapWritable> {
	private static final transient Logger LOG = LoggerFactory.getLogger(StripesHashTagCoOccMapper.class);
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Properties properties = new Properties();
	private MapWritable map = new MapWritable();
	private int hash_tags_key;
	private int hash_tag_count_key;

	protected void setup(Context context) {
		try {
			String stopwordCacheName = new Path("/conf/headers_twitterData.properties").getName();
			File file = new File(stopwordCacheName);
			Path cachePath = new Path(file.getAbsolutePath());
			if (cachePath.getName().equals(stopwordCacheName)) {
				properties.load(new FileInputStream(cachePath.toString()));
			}
			hash_tags_key = Integer.parseInt(properties.getProperty("HASH_TAGS")) - 1;
			hash_tag_count_key = Integer.parseInt(properties.getProperty("HASH_TAG_COUNT")) - 1;
		} catch (IOException e) {
			LOG.error(e.getStackTrace().toString());
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] contents = line.split("\t");
		int numTags = 0;
		try {
			numTags = Integer.parseInt(contents[hash_tag_count_key]);
		} catch (NumberFormatException e) {

		}
		if (numTags >= 2) {
			String hashTags = contents[hash_tags_key].toLowerCase();
			String[] hashTagArr = hashTags.split(",");
			for (int i = 0; i < 2; i++) {
				String term = hashTagArr[i];

				map.clear();

				if (map.containsKey(hashTagArr[(i + 1) % 2])) {
					IntWritable count = (IntWritable) map.get(hashTagArr[(i + 1) % 2]);
					count.set(count.get() + 1);
					map.put(new Text(hashTagArr[(i + 1) % 2]), count);
				} else {
					map.put(new Text(hashTagArr[(i + 1) % 2]), one);
				}

				word.set(term);
				context.write(word, map);
			}
		}
	}
}