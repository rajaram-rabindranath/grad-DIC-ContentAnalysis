package userreferencecount.postprocess;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class UserReferenceCountPostProcess {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String doc = readTextFile("output/userreferencecount.txt");
		String[] lineArr = doc.split("\n");
		JSONArray cloud = new JSONArray();
		for (String line : lineArr) {
			String[] inputLine = line.split("\t");
			int count = Integer.parseInt(inputLine[0]);
			String word = inputLine[1];
			JSONObject tag = new JSONObject();
			try {
				tag.put("text", word);
				tag.put("size", count);
				cloud.put(tag);
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		
		try {
			FileWriter file = new FileWriter("userreferencecount.json");
			file.write(cloud.toString(5));
			file.flush();
			file.close();

		} catch (JSONException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static String readTextFile(String fileName) {

		String returnValue = "";
		FileReader file = null;

		try {
			file = new FileReader(fileName);
			BufferedReader reader = new BufferedReader(file);
			String line = "";
			int ct = 0;
			while ((line = reader.readLine()) != null && ct < 50) {
				returnValue += line + "\n";
				ct++;
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
}
