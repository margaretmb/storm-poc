package storm.cookbook;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class ClickSpout extends BaseRichSpout {

	private static final long serialVersionUID = 3457693188404434815L;
	
	private SpoutOutputCollector collector;
	private static final MongoDriver mongo = new MongoDriver();
	private List<DBObject> allClicks;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		DB db = mongo.getDb();
		DBCollection clicks = db.getCollection("clicks");
		allClicks = clicks.find().toArray();
		
	}

	@Override
	public void nextTuple() {
		
		DB db = mongo.getDb();
		DBCollection movies = db.getCollection("movies");
		DBCollection users = db.getCollection("users");
		
		DBObject click = null;
		if (!allClicks.isEmpty()) {
			click = allClicks.get(allClicks.size() - 1);
		}
		
		if (click != null) {	
			
			DBObject userIdObject = new BasicDBObject("_id", click.get("userId"));
			DBObject user = users.findOne(userIdObject);
			String userName = user.get("name").toString();
			
			DBObject movieIdObject = new BasicDBObject("_id", click.get("movieId"));
			DBObject movie = movies.findOne(movieIdObject);
			String movieName = movie.get("name").toString();
			
			collector.emit(new Values(userName, movieName));
		} 
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("userId", "movieId"));
		
	}

}
