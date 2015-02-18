package shortestPath.postprocess;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class PostProcessOutput {

	public static void main(String[] args) {
		String doc = readTextFile("output/bina/graph_20/part-r-00000");
		String[] lineArr = doc.split("\n");
		HashSet<String> nodeSet = new HashSet<String>();
		JSONObject obj = new JSONObject();
		JSONArray nodeList = new JSONArray();
		JSONArray edgeList = new JSONArray();
		int edge_id = 0;
		int line_no = 0;
		try {
			for (String line : lineArr) {
				String[] inputLine = line.split("\t");
				if (inputLine.length == 2) {
					String source = inputLine[0];
					String all = inputLine[1];
					String[] allArr = all.split(";");
					String targets = allArr[0];
					String[] targetArr = targets.split(",");
					String dist = allArr[1];
					if (!nodeSet.contains(source)) {
						nodeSet.add(source);
						JSONObject node = new JSONObject();
						node.put("id", source);
						node.put("label", source + " d=" + dist);
						node.put("size", targetArr.length);
						if (line_no == 0)
							node.put("color", "#f00");
						else
							node.put("color", "#00f");
						nodeList.put(node);
					}
					for (String target : targetArr) {
						JSONObject edge = new JSONObject();
						edge.put("id", "id" + edge_id);
						edge.put("source", source);
						edge.put("target", target);
						edgeList.put(edge);
						edge_id++;
					}
				}
				line_no++;
			}
			obj.put("nodes", nodeList);
			obj.put("edges", edgeList);
		} catch (JSONException e) {
			e.printStackTrace();
		}

		try {
			FileWriter file = new FileWriter("outdata.json");
			file.write(obj.toString(5));
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
			while ((line = reader.readLine()) != null) {
				returnValue += line + "\n";
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
