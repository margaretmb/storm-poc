package storm.cookbook;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ClickCounterBolt extends BaseRichBolt {

	private static final long serialVersionUID = 8251135323475321397L;
	private static Map<String, Integer> movieCount = new HashMap<String, Integer>();
	private static Map<String, Integer> userCount = new HashMap<String, Integer>();
	private OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;

	}

	@Override
	public void execute(Tuple input) {

		List<ClickSpout.ClickEvent> events = (List<ClickSpout.ClickEvent>) input.getValueByField("clicks");

		for (ClickSpout.ClickEvent event : events) {
			String movieId = event.getMovieName();
			if (movieCount.containsKey(movieId)) {
				int count = movieCount.get(movieId);
				count++;
				movieCount.put(movieId, count);
			} else {
				movieCount.put(movieId, 1);
			}

			String userId = event.getUserName();
			if (userCount.containsKey(userId)) {
				int count = userCount.get(userId);
				count++;
				userCount.put(userId, count);
			} else {
				userCount.put(userId, 1);
			}
			
			System.out.println("user " + userId + " has clicked movies " + userCount.get(userId) + " times.");
			System.out.println("movie " + movieId + " has been clicked " + movieCount.get(movieId) + " times.");

		}

		collector.emit(new Values(movieCount, userCount));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("movieCounts", "userCounts"));
	}

}
