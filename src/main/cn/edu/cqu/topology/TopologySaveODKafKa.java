package cn.edu.cqu.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Nimbus;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import cn.edu.cqu.kafka_serial_deserial.UserScheme;
import cn.edu.cqu.od.SavaODBolt;
import org.json.simple.JSONValue;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;

/**
 * Created by lab on 2016/5/29.
 *
 * @author lx
 */
public class TopologySaveODKafKa {

    private static KafkaSpout kafkaSpout() {
        String zks = "node1:2181,node2:2181,node3:2181,node4:2181,node5:2181";
        String topic = "od-for-week";
        String zkRoot = "/consumers";
        String id = UUID.randomUUID().toString();  //group id
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        //Set KafkaSpout's output using self-defined UserScheme.
        spoutConfig.scheme = new SchemeAsMultiScheme(new UserScheme());
        spoutConfig.zkServers = Arrays.asList("node1", "node2", "node3", "node4", "node5");
        spoutConfig.zkPort = 2181;
        //read from the end of the topic
        //spoutConfig.startOffsetTime= kafka.api.OffsetRequest.LatestTime();

        return new KafkaSpout(spoutConfig);
    }

    public static void main(String[] args) throws Exception {
        //Topology Definition
        /**
         * read od result from kafka and save it into database on node1.
         */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("od-kafka", kafkaSpout(), 5);
        builder.setBolt("save-od-kafka-eid", new SavaODBolt(), 10).shuffleGrouping("od-kafka");

        //Topology Configuration
        /**
         * For the sake of efficiency, we close acker bolt;
         * here setNumAckers=0 means when spout emit a tuple, it will execute ack()
         * immediately.
         */
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(3);
        conf.setNumAckers(1);
        conf.setMaxSpoutPending(500);
        conf.setMessageTimeoutSecs(30);

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            //read storm's local configuration

            //set nimbus's ip
            String nimbus_host = "node1_IP";
            String topology_name = "save_kafka_od";
            Map<String, Object> stormConf = Utils.readStormConfig();
            stormConf.put("nimbus.host", nimbus_host);
            stormConf.putAll(conf);

            Nimbus.Client client = NimbusClient.getConfiguredClient(stormConf).getClient();
            //jar file's path
            String inputJar = ".\\target\\storm-od-1.0-SNAPSHOT-jar-with-dependencies.jar";
            NimbusClient nimbusClient = new NimbusClient(stormConf, nimbus_host, 6627); //thrift port

            //submit jar file with StormSubmitter to nimbus
            String uploadedJarLocation = StormSubmitter.submitJar(stormConf, inputJar);
            String jsonConf = JSONValue.toJSONString(stormConf);
            nimbusClient.getClient().submitTopology(topology_name, uploadedJarLocation, jsonConf, builder.createTopology());
        }
    }
}
