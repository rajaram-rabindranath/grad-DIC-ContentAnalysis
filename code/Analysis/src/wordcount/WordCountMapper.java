package wordcount;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final transient Logger LOG = LoggerFactory.getLogger(WordCountMapper.class);
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Properties properties = new Properties();
	private int tweet_lang_key;
	private int tweet_key;

	protected void setup(Context context) {
		try {
			String stopwordCacheName = new Path("/conf/headers_twitterData.properties").getName();
			File file = new File(stopwordCacheName);
			Path cachePath = new Path(file.getAbsolutePath());
			if (cachePath.getName().equals(stopwordCacheName)) {
				properties.load(new FileInputStream(cachePath.toString()));
			}

			tweet_lang_key = Integer.parseInt(properties.getProperty("TWEET_LANG")) - 1;
			tweet_key = Integer.parseInt(properties.getProperty("TWEET")) - 1;
		} catch (IOException e) {
			LOG.error(e.getStackTrace().toString());
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] contents = line.split("\t");
		LOG.debug("Size: " + contents.length + "Contents: " + line);
		System.out.println(tweet_lang_key + " " + tweet_key + " " + contents[tweet_lang_key]);
		if (contents[tweet_lang_key].equals("en")) {
			String tweet = contents[tweet_key];
			StringTokenizer itr = new StringTokenizer(filterString(tweet));
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken().toLowerCase());
				context.write(word, one);
			}
		}
	}

	private static String filterString(String input) {
		if (input != null) {
			String output = input.trim().toLowerCase().replaceAll("[^a-z0-9\\s\\p{L}]", " ");
			output = output.replaceAll("\\s{2,}", " ");
			return output;
		}
		return null;
	}
}