package wordcount.postprocess;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class PostProcess {
	private static List<String> stopList;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		loadStopWordList();
		System.out.println(stopList);
		String doc = readTextFile("output/wordcount.txt", true);

		String[] lineArr = doc.split("\n");
		JSONArray cloud = new JSONArray();
		for (String line : lineArr) {
			String[] inputLine = line.split("\t");
			int count = Integer.parseInt(inputLine[0]);
			String word = inputLine[1];
			System.out.println("Word: " + word + " Contains: " + stopList.contains(word.trim()));
			if (!stopList.contains(word) && !word.matches("\\d+")) {
				JSONObject tag = new JSONObject();
				try {
					tag.put("text", word);
					tag.put("size", count);
					cloud.put(tag);
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
		}

		try {
			FileWriter file = new FileWriter("wordcount.json");
			file.write(cloud.toString(5));
			file.flush();
			file.close();

		} catch (JSONException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String readTextFile(String fileName, boolean readFew) {

		String returnValue = "";
		FileReader file = null;

		try {
			file = new FileReader(fileName);
			BufferedReader reader = new BufferedReader(file);
			String line = "";
			if (readFew) {
				int ct = 0;
				while ((line = reader.readLine()) != null && ct < 300) {
					returnValue += line + "\n";
					ct++;
				}
			}
			else{
				while ((line = reader.readLine()) != null) {
					returnValue += line + "\n";
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (file != null) {
				try {
					file.close();
				} catch (IOException e) {
				}
			}
		}
		return returnValue;
	}

	private static void loadStopWordList() {
		String doc = readTextFile("data/stopList.txt", false);
		String[] wordArr = doc.split("[\n\r\f]");
		stopList = (List<String>) Arrays.asList(wordArr);
	}
}
