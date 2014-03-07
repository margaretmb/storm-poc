package storm.cookbook;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.QueryBuilder;

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
	private List<DBObject> newClicks;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		DB db = mongo.getDb();
		DBCollection clicks = db.getCollection("clicks");
		DBObject query = QueryBuilder.start("processed").notEquals(true).get();
		newClicks = clicks.find(query).toArray();
		
	}

	@Override
	public void nextTuple() {
		
		DB db = mongo.getDb();
		DBCollection movies = db.getCollection("movies");
		DBCollection users = db.getCollection("users");
		
		List<ClickEvent> clicks = new ArrayList<ClickEvent>();
		
		for (DBObject click : newClicks){	
			
			DBCollection clickCollection = db.getCollection("clicks");
			
			DBObject userIdObject = new BasicDBObject("_id", click.get("userId"));
			DBObject user = users.findOne(userIdObject);
			String userName = user.get("name").toString();
			
			DBObject movieIdObject = new BasicDBObject("_id", click.get("movieId"));
			DBObject movie = movies.findOne(movieIdObject);
			String movieName = movie.get("name").toString();
			
			clicks.add(new ClickEvent(movieName, userName));
			
			DBObject newClick = new BasicDBObject("_id",
					click.get("_id"))
					.append("movieId", click.get("movieId"))
					.append("userId", click.get("userId"))
					.append("processed", true);
			clickCollection.update(click, newClick);
		} 
		
		collector.emit(new Values(clicks));
		
	}
	
	public class ClickEvent {
		private String movieName;
		private String userName;
		
		public ClickEvent(String movieName, String userName) {
			this.movieName = movieName;
			this.userName = userName;
		}

		public String getMovieName() {
			return movieName;
		}

		public String getUserName() {
			return userName;
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("clicks"));
		
	}

}
