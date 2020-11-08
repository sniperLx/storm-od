package cn.edu.cqu.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Nimbus;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import cn.edu.cqu.od.GetPathChainBolt;
import cn.edu.cqu.od.ODCalcBolt;
import cn.edu.cqu.od.ODListNode;
import cn.edu.cqu.od.SaveONPEIDBolt;
import org.json.simple.JSONValue;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by lab on 2016/5/22.
 *
 * @author lx
 */
public class TopologyRemoteKafka implements Serializable {
    private static final long serialVersionUID = -6616289626193647643L;

    private static KafkaSpout kafkaSpout() {
        /**
         * Configure kafka
         */
        String zks = "node1:2181,node2:2181,node3:2181,node4:2181,node5:2181";
        String topic = "od_week";
        String zkRoot = "/od_week";
        String id = UUID.randomUUID().toString();
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        //set KafkaSpout's output, defined by StringScheme.
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList("node1", "node2", "node3", "node4", "node5");
        spoutConfig.zkPort = 2181;
        //read from the end of the topic
        //spoutConfig.startOffsetTime= kafka.api.OffsetRequest.LatestTime();

        return new KafkaSpout(spoutConfig);
    }

    private static KafkaBolt kafkaBolt() {
        KafkaBolt<String, ODListNode>
                kafkaBolt = new KafkaBolt<String, ODListNode>();
        //Tell kafkaBolt what field it will get from ODCalcBolt,
        //and save data to which topic.
        TupleToKafkaMapper
                toKafkaMapper = new FieldNameBasedTupleToKafkaMapper("eid", "single-car-od-list");
        kafkaBolt.withTupleToKafkaMapper(toKafkaMapper)
                .withTopicSelector(new DefaultTopicSelector("od-for-week"));
        return kafkaBolt;
    }

    private static Properties kafkaBoltConfig() {
        Properties kafkaBoltConf = new Properties();
        String brokers = "node1:9092,node2:9092,node3:9092,node4:9092,node5:9092";
        kafkaBoltConf.put("metadata.broker.list", brokers);
        kafkaBoltConf.put("request.required.acks", "1");
        //Assign serializer class for "K" in Producer<K,V>, where K is commonly used to decide where the V will be stored.
        kafkaBoltConf.put("key.serializer.class", "kafka.serializer.StringEncoder");
        //Assign serializer class for "V" in Producer<K,V>, where V is the message we send to brokers. Here is a self-defined serializer
        kafkaBoltConf.put("serializer.class", "cn.edu.cqu.kafka_serial_deserial.UserEncoder");

        return kafkaBoltConf;
    }

    public static void main(String[] args) throws Exception {
        /**
         * Configure topology
         *
         * For the sake of efficiency, we can close acker bolt.
         * Setting setNumAckers=0 means when spout emit a tuple, it will execute ack()
         * immediately.
         */
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(7);
        conf.setNumAckers(2);
        conf.setMaxSpoutPending(500);
        conf.setMessageTimeoutSecs(30);
        conf.put("kafka.broker.properties", kafkaBoltConfig());
        conf.put("topic", "od-for-week");

        /**
         * Define storm topology
         */
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("get-eid", kafkaSpout(), 6);
        builder.setBolt("get-path-chain-bolt", new GetPathChainBolt(), 15)
                .shuffleGrouping("get-eid");
        builder.setBolt("save-onpEID-bolt", new SaveONPEIDBolt(), 2)
                .directGrouping("get-path-chain-bolt", "oneNodePath");
        builder.setBolt("calc-single-car-od", new ODCalcBolt(), 12)
                .shuffleGrouping("get-path-chain-bolt");
        builder.setBolt("save-od-to-kafka", kafkaBolt(), 4)
                .shuffleGrouping("calc-single-car-od");


        /**
         * Submit the topology
         */
        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {  //read storm's local configuration
            String nimbus_host = "node1_IP";    //set nimbus's ip
            String topology_name = "od-calc";
            Map<String, Object> stormConf = Utils.readStormConfig();
            stormConf.put("nimbus.host", nimbus_host);
            stormConf.putAll(conf);

            Nimbus.Client client = NimbusClient.getConfiguredClient(stormConf).getClient();
            //jar file's path
            String inputJar = "./target/storm-od-1.0-SNAPSHOT-jar-with-dependencies.jar";
            NimbusClient nimbusClient = new NimbusClient(stormConf, nimbus_host, 6627); //thrift port

            //submit jar file with StormSubmitter to nimbus
            String uploadedJarLocation = StormSubmitter.submitJar(stormConf, inputJar);
            String jsonConf = JSONValue.toJSONString(stormConf);
            nimbusClient.getClient().submitTopology(topology_name, uploadedJarLocation, jsonConf, builder.createTopology());
        }
    }
}
