package shortestPath;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import shortestPath.ShortestPathDriver.Iterations;

public class ShortestPathReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Node outputNode = new Node();
		outputNode.setNode_id(key.toString());
		int init_cost = outputNode.getCost();
		String init_color = outputNode.getColor();
		String GRAY = Node.getColorList().get(1);

		for (Text value : values) {
			Node inputNode = new Node(key.toString() + "\t" + value.toString());
			HashSet<String> adjacency_list = inputNode.getNeighbors();
			int adjacency_list_size = adjacency_list.size();
			int new_cost = inputNode.getCost();
			String parent_node = inputNode.getSource();
			String new_color = inputNode.getColor();
			if (adjacency_list_size > 0)
				outputNode.setNeighbors(adjacency_list);
			if (new_cost < init_cost) {
				outputNode.setCost(new_cost);
				outputNode.setSource(parent_node);
			}
			if (Node.getColorList().indexOf(new_color) > Node.getColorList().indexOf(init_color))
				outputNode.setColor(new_color);
				init_color = outputNode.getColor();

		}
		context.write(key, new Text(outputNode.toString()));

		if (init_color.equals(GRAY))
			context.getCounter(Iterations.numIter).increment(1L);

	}
}