package relative_tagcoocc.pairs;

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

public class PairsRelativeHashTagCoOccMapper extends Mapper<Object, Text, Text, IntWritable> {
	private static final transient Logger LOG = LoggerFactory.getLogger(PairsRelativeHashTagCoOccMapper.class);
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Properties properties = new Properties();
	private int hash_tags_key;
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

			hash_tags_key = Integer.parseInt(properties.getProperty("HASH_TAGS")) - 1;
			hash_tag_count_key = Integer.parseInt(properties.getProperty("HASH_TAG_COUNT")) - 1;
			analyze_all_tweets = Boolean.parseBoolean(properties.getProperty("analyze_all_tweets"));
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
			if (analyze_all_tweets) {
				for (int i = 0; i < hashTagArr.length; i++) {
					String term = hashTagArr[i].trim();
					if (term.length() > 0) {
						for (int j = 0; j < hashTagArr.length; j++) {
							if (i != j) {
								String term2 = hashTagArr[j].trim();
								if (term2.length() > 0) {
									word.set(term + "," + term2);
									context.write(word, one);

									word.set(term + "," + "*");
									context.write(word, one);
								}
							}
						}
					}
				}
			} else {
				for (int i = 0; i < 2; i++) {
					String term = hashTagArr[i].trim();
					if (term.length() > 0) {
						String term2 = hashTagArr[(i + 1) % 2].trim();
						if (term2.length() > 0) {
							word.set(term + "," + term2);
							context.write(word, one);

							word.set(term + "," + "*");
							context.write(word, one);
						}
					}
				}
			}
		}
	}
}