package shortestPath;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ShortestPathMapper extends Mapper<Object, Text, Text, Text> {

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Node inputNode = new Node(value.toString());
		String GRAY = Node.getColorList().get(1);
		if (inputNode.getColor().equals(GRAY)) {
			for (String neighbors : inputNode.getNeighbors()) {
				Node neighbor = new Node();
				neighbor.setNode_id(neighbors);
				int old_cost = inputNode.getCost();
				int new_cost = old_cost + 1;
				neighbor.setCost(new_cost);
				String new_color = Node.getColorList().get(1);
				neighbor.setColor(new_color);
				neighbor.setSource(inputNode.getNode_id());

				context.write(new Text(neighbor.getNode_id()), new Text(neighbor.toString()));
			}
			inputNode.setColor(Node.getColorList().get(2));
		}
		context.write(new Text(inputNode.getNode_id()), new Text(inputNode.toString()));
	}
}