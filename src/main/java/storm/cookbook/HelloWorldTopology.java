package storm.cookbook;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class HelloWorldTopology {
	
	private final static MongoDriver mongo = new MongoDriver();
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		handleMongo();
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("randomHelloWorld", new HelloWorldSpout(), 10);
		builder.setBolt("HelloWorldBolt", new HelloWorldBolt(), 3)
		.shuffleGrouping("randomHelloWorld");
		
		Config conf = new Config();
		conf.setDebug(true);
		
		if (args != null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			Utils.sleep(10000);
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}
	
	private static void handleMongo() {
		mongo.resetMongo();
		
		Runnable mongoRunner = new Runnable() {

			@Override
			public void run() {
				mongo.generateRandomClick();
			}

		};

		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(mongoRunner, 0, 3, TimeUnit.SECONDS);
	}

}
