package shortestPath.preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class PreProcess {

	public static void main(String[] args) {
		String doc = readTextFile("input/graphData/input-graph-large");
		String[] lineArr = doc.split("\n");
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter("input/graphData/input-graph.txt"));
			for (String line : lineArr) {
				String[] inputLine = line.split(" ");
				String source = inputLine[0];
				String targets = inputLine[2];
				String[] targetArr = targets.split(":");
				bw.write(source + ":");
				int count=0;
				for (String target : targetArr) {
					if(target.length()>0){
						if(count != 0)
							bw.write(",");
						bw.write(target);
					}
					count++;
				}
				bw.write("\n");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			if (bw != null) {
				try {
					bw.close();
				} catch (IOException e) {
				}
			}
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
