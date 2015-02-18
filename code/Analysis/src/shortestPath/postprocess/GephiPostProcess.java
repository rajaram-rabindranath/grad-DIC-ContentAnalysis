package shortestPath.postprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

public class GephiPostProcess {

	public static void main(String[] args){
		BufferedReader br = null;
		HashSet<Long> members = new HashSet<Long>();
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader("output/SSSP/UBCommunity.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				members.add(Long.parseLong(sCurrentLine));
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		HashSet<String> nodeSet = new HashSet<String>();
		BufferedWriter bw_node = null;
		BufferedWriter bw_edge = null;
		int node_id = 0;
		int edge_id = 0;
		try {
			bw_node = new BufferedWriter(new FileWriter("output/Nodes.csv"));
			bw_node.write("node,id,label");
			bw_node.write("\n");
			bw_edge = new BufferedWriter(new FileWriter("output/Edges.csv"));
			bw_edge.write("source,target,type,id");
			bw_edge.write("\n");
			String doc = readTextFile("input/followers.txt");
			String[] lineArr = doc.split("\n");
			for(String line : lineArr){
				String[] inputLine = line.split(":");
				if (inputLine.length == 2) {
					String source = inputLine[0];
					String targets = inputLine[1];
					String[] targetArr = targets.split(",");
					if (!nodeSet.contains(source)) {
						nodeSet.add(source);
						bw_node.write(source + "," + node_id + "," + source);
						bw_node.write("\n");
						node_id++;
					}
					for (String target : targetArr) {
						if (!nodeSet.contains(target)){
							nodeSet.add(target);
							bw_node.write(target + "," + node_id + "," + target);
							bw_node.write("\n");
							node_id++;
						}
						bw_edge.write(source + "," + target + "," + "Undirected" + "," + edge_id);
						bw_edge.write("\n");
						edge_id++;
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally {
			if (bw_edge != null) {
				try {
					bw_edge.close();
				} catch (IOException e) {
				}
			}
			if (bw_node != null) {
				try {
					bw_node.close();
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
