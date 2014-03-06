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

public class ClickTopology {
	
	private final static MongoDriver mongo = new MongoDriver();
	
	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
		handleMongo();
		
		handleStorm(args);
	}

	protected static void handleStorm(final String[] args)
			throws AlreadyAliveException, InvalidTopologyException {
		
		Runnable stormRunner = new Runnable() {

			@Override
			public void run() {
				TopologyBuilder builder = new TopologyBuilder();
				builder.setSpout("randomHelloWorld", new ClickSpout(), 10);
				builder.setBolt("HelloWorldBolt", new ClickCounterBolt(), 3)
				.shuffleGrouping("randomHelloWorld");
				
				Config conf = new Config();
				conf.setDebug(true);
				
				if (args != null && args.length > 0) {
					conf.setNumWorkers(3);
					try {
						StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
					} catch (AlreadyAliveException e) {
						e.printStackTrace();
					} catch (InvalidTopologyException e) {
						e.printStackTrace();
					}
				} else {
					LocalCluster cluster = new LocalCluster();
					cluster.submitTopology("test", conf, builder.createTopology());
					Utils.sleep(10000);
					cluster.killTopology("test");
					cluster.shutdown();
				}
			}
			
		};
		
		ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
		executor.scheduleAtFixedRate(stormRunner, 0, 3, TimeUnit.SECONDS);
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
		executor.scheduleAtFixedRate(mongoRunner, 0, 1, TimeUnit.SECONDS);
	}

}
