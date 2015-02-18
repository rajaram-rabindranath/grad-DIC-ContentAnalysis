package shortestPath;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class Node {

	private static ArrayList<String> colorList = new ArrayList<String>(Arrays.asList("WHITE", "GRAY", "BLACK"));
	private String node_id;
	private int cost;
	private HashSet<String> neighbors = new HashSet<String>();
	private String color = colorList.get(0);
	private String source;

	public Node() {
		cost = Integer.MAX_VALUE;
		color = colorList.get(0);
		source = null;
	}

	public Node(String node) {
		String[] inputArr = node.split("\t");
		this.node_id = inputArr[0];
		String[] tokens = inputArr[1].split(";");
		for (String token : tokens[0].split(",")) {
			if (token.length() > 0 && !token.equals("NULL")) {
				neighbors.add(token);
			}
		}
		this.cost = Integer.parseInt(tokens[1]);
		this.color = tokens[2];
		this.source = tokens[3];
	}

	@Override
	public String toString() {
		StringBuffer buffer = new StringBuffer();
		for (String neighbor : neighbors) {
			if (buffer.length() > 0)
				buffer.append(",");
			buffer.append(neighbor);

		}
		if (neighbors.size() == 0)
			buffer.append("NULL");

		buffer.append(";");

		if (this.cost < Integer.MAX_VALUE) {
			buffer.append(this.cost);
			buffer.append(";");
		} else {
			buffer.append(String.valueOf(Integer.MAX_VALUE));
			buffer.append(";");
		}
		buffer.append(color);
		buffer.append(";");
		buffer.append(this.source);

		return buffer.toString();
	}

	/**
	 * @return the colorList
	 */
	public static ArrayList<String> getColorList() {
		return colorList;
	}

	/**
	 * @param colorList
	 *            the colorList to set
	 */
	public static void setColorList(ArrayList<String> colorList) {
		Node.colorList = colorList;
	}

	/**
	 * @return the node_id
	 */
	public String getNode_id() {
		return node_id;
	}

	/**
	 * @param node_id
	 *            the node_id to set
	 */
	public void setNode_id(String node_id) {
		this.node_id = node_id;
	}

	/**
	 * @return the cost
	 */
	public int getCost() {
		return cost;
	}

	/**
	 * @param cost
	 *            the cost to set
	 */
	public void setCost(int cost) {
		this.cost = cost;
	}

	/**
	 * @return the neighbors
	 */
	public HashSet<String> getNeighbors() {
		return neighbors;
	}

	/**
	 * @param neighbors
	 *            the neighbors to set
	 */
	public void setNeighbors(HashSet<String> neighbors) {
		this.neighbors = neighbors;
	}

	/**
	 * @return the color
	 */
	public String getColor() {
		return color;
	}

	/**
	 * @param color
	 *            the color to set
	 */
	public void setColor(String color) {
		this.color = color;
	}

	/**
	 * @return the source
	 */
	public String getSource() {
		return source;
	}

	/**
	 * @param source
	 *            the source to set
	 */
	public void setSource(String source) {
		this.source = source;
	}
}