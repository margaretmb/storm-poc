package storm.cookbook;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class MostClickedBolt extends BaseRichBolt {

	private static final long serialVersionUID = 3341493673633780990L;
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		Map<String, Integer> movieCounts = (Map<String, Integer>) input.getValueByField("movieCounts");
		Map<String, Integer> userCounts = (Map<String, Integer>) input.getValueByField("userCounts");
				
		String currentWinner = "";
		int currentMostVotes = Integer.MIN_VALUE;
		
		for (String movie : movieCounts.keySet()) {
			int votes = movieCounts.get(movie);
			if (votes > currentMostVotes) {
				currentWinner = movie;
				currentMostVotes = votes;
			}
		}
		
		System.out.println(currentWinner + " currently has the most votes.");
		
		String currentProlificUser = "";
		int currentMostVoted = Integer.MIN_VALUE;
		
		for (String user : userCounts.keySet()) {
			int votes = userCounts.get(user);
			if (votes > currentMostVoted) {
				currentProlificUser = user;
				currentMostVoted = votes;
			}
		}
		
		System.out.println(currentProlificUser + " has done the most clicking.");
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

}
