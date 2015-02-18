package shortestPath.preprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class BFSPrepare {

	public static void main(String[] args) {
		BufferedReader br = null;
		BufferedWriter bw = null;
		String[] colors = { "WHITE", "GRAY", "BLACK" };

		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader("input/graphData/input-graph.txt"));
			bw = new BufferedWriter(new FileWriter("input/input-graph.txt"));
			int count = 0;
			while ((sCurrentLine = br.readLine()) != null) {
				String[] inputLine = sCurrentLine.split(":");
				if (inputLine.length == 2) {
					bw.write(inputLine[0]);
					bw.write("\t");
					bw.write(inputLine[1]);
					int distance = Integer.MAX_VALUE;
					String color = colors[0];
					String parent = "null";
					if (count == 0) {
						distance = 0;
						color = colors[1];
						parent = "source";
					}
					bw.write(";");
					bw.write(String.valueOf(distance));
					bw.write(";");
					bw.write(color);
					bw.write(";");
					bw.write(parent);
					bw.write("\n");
				}
				count++;
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
				if (bw != null)
					bw.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}

}
