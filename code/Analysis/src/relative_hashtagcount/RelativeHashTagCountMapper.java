package relative_hashtagcount;

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

public class RelativeHashTagCountMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final transient Logger LOG = LoggerFactory.getLogger(RelativeHashTagCountMapper.class);
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Properties properties = new Properties();
	private int hashtag_key;
	private int hash_tag_count_key;
	private boolean analyze_all_tweets;

	protected void setup(Context context) {
		try {
			String stopwordCacheName = new Path("/conf/headers_twitterData.properties").getName();
			File file = new File(stopwordCacheName);
			Path cachePath = new Path(file.getAbsolutePath());
			if (cachePath.getName().equals(stopwordCacheName)) {
				properties.load(new FileInputStream(cachePath.toString()));
			}

			hashtag_key = Integer.parseInt(properties.getProperty("HASH_TAGS")) - 1;
			hash_tag_count_key = Integer.parseInt(properties.getProperty("HASH_TAG_COUNT")) - 1;
			analyze_all_tweets = Boolean.parseBoolean(properties.getProperty("analyze_all_tweets"));
		} catch (IOException e) {
			LOG.error(e.getStackTrace().toString());
		}
	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] contents = line.split("\t");
		LOG.debug("Size: " + contents.length + "Contents: " + line);
		System.out.println(hashtag_key);
		int numTags = 0;
		try {
			numTags = Integer.parseInt(contents[hash_tag_count_key]);
		} catch (NumberFormatException e) {

		}
		if (numTags > 0) {
			String hashtags = contents[hashtag_key].toLowerCase();
			String[] hashtagArr = hashtags.split(",");
			if (hashtagArr.length >= 2) {
				if (analyze_all_tweets) {
					for (String hashtag : hashtagArr) {
						String term = hashtag.trim();
						if (term.length() > 0) {
							word.set(term);
							for (int i = 0; i < hashtagArr.length - 1; i++)
								context.write(word, one);
						}
					}
				} else {
					int count = 0;
					for (String hashtag : hashtagArr) {
						if (count == 2)
							break;
						String term = hashtag.trim();
						if (term.length() > 0) {
							word.set(term);
							context.write(word, one);
						}
						count++;
					}
				}
			}
		}
	}
}