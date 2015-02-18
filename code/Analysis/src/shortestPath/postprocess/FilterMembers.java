package shortestPath.postprocess;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;

public class FilterMembers {

	public static void main(String[] args) {
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
		BufferedWriter bw = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader("output/SSSP/sssp.txt"));
			bw = new BufferedWriter(new FileWriter("output/SSSP/final.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				String[] inputLine = sCurrentLine.split("\t");
				if(members.contains(Long.parseLong(inputLine[0]))){
					bw.write(sCurrentLine);
					bw.write("\n");
				}
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
