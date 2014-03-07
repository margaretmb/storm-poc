package storm.cookbook;

import java.io.File;
import java.net.UnknownHostException;
import java.util.List;

import com.mongodb.*;

public class MongoDriver {

	private MongoClient mongoClient;
	private DB db;
	public static final int MONGO_PORT = 27017;

	public MongoDriver() {
		try {
			mongoClient = new MongoClient("localhost", MONGO_PORT);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		
		db = mongoClient.getDB("spark");
	}
	
	public void resetMongo() {
		db.dropDatabase();

		DBCollection movies = db.getCollection("movies");
		BasicDBObject movie1 = new BasicDBObject("name", "Back to the Future");
		BasicDBObject movie2 = new BasicDBObject("name", "The Avengers");
		BasicDBObject movie3 = new BasicDBObject("name", "The Hobbit");

		movies.insert(new BasicDBObject[] { movie1, movie2, movie3 });

		DBCollection users = db.getCollection("users");
		BasicDBObject user1 = new BasicDBObject("name", "Margaret");
		BasicDBObject user2 = new BasicDBObject("name", "Kartik");
		BasicDBObject user3 = new BasicDBObject("name", "Tim");
		BasicDBObject user4 = new BasicDBObject("name", "Michele");

		users.insert(new BasicDBObject[] { user1, user2, user3, user4 });

		File moviesFile = new File("text/movies.txt");
		if (moviesFile.exists()) {
			moviesFile.delete();
		}
	}

	public void generateRandomClick() {
		DBCollection userCollection = db.getCollection("users");
		DBCollection movieCollection = db.getCollection("movies");

		long randomUserIndex = (long) Math.floor(Math.random()
				* userCollection.count());
		long randomMovieIndex = (long) Math.floor(Math.random()
				* movieCollection.count());

		List<DBObject> users = userCollection.find().toArray();
		List<DBObject> movies = movieCollection.find().toArray();

		DBObject randomUser = users.get((int) randomUserIndex);
		DBObject randomMovie = movies.get((int) randomMovieIndex);

		BasicDBObject randomClick = new BasicDBObject("userId",
				randomUser.get("_id"))
				.append("movieId", randomMovie.get("_id"))
				.append("processed", false);

		DBCollection clicks = db.getCollection("clicks");
		clicks.insert(randomClick);

		System.out.println(randomUser.get("name") + " clicked on "
				+ randomMovie.get("name"));
	}

	public DB getDb() {
		return db;
	}

}
