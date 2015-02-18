package authorcount;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthorCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final transient Logger LOG = LoggerFactory.getLogger(AuthorCountMapper.class);
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Properties properties = new Properties();
	private int screenname_key;

	protected void setup(Context context) {
		try {
			String stopwordCacheName = new Path("/conf/headers_twitterData.properties").getName();
			File file = new File(stopwordCacheName);
			Path cachePath = new Path(file.getAbsolutePath());
			if (cachePath.getName().equals(stopwordCacheName)) {
				properties.load(new FileInputStream(cachePath.toString()));
			}

			screenname_key = Integer.parseInt(properties.getProperty("SCREEN_NAME")) - 1;
		} catch (IOException e) {
			LOG.error(e.getStackTrace().toString());
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] contents = line.split("\t");
		LOG.debug("Size: " + contents.length + "Contents: " + line);
		System.out.println(screenname_key);
		String screen_name = contents[screenname_key];
		word.set(screen_name);
		context.write(word, one);
	}
}