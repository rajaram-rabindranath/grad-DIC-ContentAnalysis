package userreferencecount;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserReferenceCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final transient Logger LOG = LoggerFactory.getLogger(UserReferenceCountMapper.class);
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Properties properties = new Properties();
	private int tweet_key;

	protected void setup(Context context) {
		try {
			String stopwordCacheName = new Path("/conf/headers_twitterData.properties").getName();
			File file = new File(stopwordCacheName);
			Path cachePath = new Path(file.getAbsolutePath());
			if (cachePath.getName().equals(stopwordCacheName)) {
				properties.load(new FileInputStream(cachePath.toString()));
			}

			tweet_key = Integer.parseInt(properties.getProperty("TWEET")) - 1;
		} catch (IOException e) {
			LOG.error(e.getStackTrace().toString());
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] contents = line.split("\t");
		LOG.debug("Size: " + contents.length + "Contents: " + line);
		System.out.println(tweet_key);
		String tweet = contents[tweet_key];
		Pattern pattern = Pattern.compile(".*?@([a-zA-Z0-9_]{1,15}).*?");
		Matcher matcher_2 = pattern.matcher(tweet);
		while (matcher_2.find()) {
			word.set(matcher_2.group(1));
			context.write(word, one);
		}
	}
}