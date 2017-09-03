package kafkaTest;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Nimbus;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.json.simple.JSONValue;
import storm.kafka.*;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.mapper.TupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by lab on 2016/5/29.
 *
 * @author lx
 */
public class TopologyKafKa {
    public static void main(String[] args) throws Exception {
        String zks = "node1:2181,node2:2181,node3:2181,node4:2181,node5:2181";
        String topic = "od_week";
        String zkRoot = "/consumers";
        String id = UUID.randomUUID().toString();  //group id
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        //set KafkaSpout's output, defined by StringScheme.
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
//        spoutConfig.forceFromStart = true;
        spoutConfig.zkServers = Arrays.asList("node1", "node2", "node3", "node4", "node5");
        spoutConfig.zkPort = 2181;
        //read from the end of the topic
        spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();

        //Configure topology
        /**
         * for the sake of effecient, we close acker bolt;
         * here setNumAckers=0 means when spout emit a tuple, it will execute ack()
         * immediately.
         */
        Properties kafkaBoltConf = new Properties();
        String brokers = "node1:9092,node2:9092,node3:9092,node4:9092,node5:9092";
        kafkaBoltConf.put("metadata.broker.list", brokers);
        kafkaBoltConf.put("request.required.acks", "1");
        kafkaBoltConf.put("key.serializer.class", "kafka_serial_deserial.serializer.StringEncoder");
        kafkaBoltConf.put("serializer.class", "cn.edu.cqu.kafka_serial_deserial.UserEncoder");
        kafkaBoltConf.put("request.required.acks", "1");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(3);
        conf.setNumAckers(1);
        conf.setMaxSpoutPending(1000);
        conf.setMessageTimeoutSecs(30);
        conf.put("kafka_serial_deserial.broker.properties", kafkaBoltConf);
        conf.put("topic", "kafka_serial_deserial-test");        // 配置KafkaBolt生成的topic

        //define topology
        KafkaBolt kafkaBolt = new KafkaBolt();
        //tell kafkaBolt what field it will get.
        TupleToKafkaMapper
                toKafkaMapper = new FieldNameBasedTupleToKafkaMapper("str", "str");
        kafkaBolt.withTupleToKafkaMapper(toKafkaMapper)
                .withTopicSelector(new DefaultTopicSelector("kafka_serial_deserial-test"));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka_serial_deserial-eid", new KafkaSpout(spoutConfig), 2);
        //builder.setBolt("save_kafka_eid", new CosumeBolt()).shuffleGrouping("kafka_eid");
        builder.setBolt("bolt-test", kafkaBolt, 2)
                .shuffleGrouping("kafka_serial_deserial-eid");


        if (args.length > 0) {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            //read storm's local configuration

            //set nimbus's ip
            String nimbus_host = "222.198.138.113";
            String topology_name = "kafka_eid";
            Map<String, Object> stormConf = Utils.readStormConfig();
            stormConf.put("nimbus.host", nimbus_host);
            stormConf.putAll(conf);

            Nimbus.Client client = NimbusClient.getConfiguredClient(stormConf).getClient();
            //jar file's path
            String inputJar = "E:\\Documents\\IdeaProjects\\storm-od\\target\\storm-od-1.0-SNAPSHOT-jar-with-dependencies.jar";
            NimbusClient nimbusClient = new NimbusClient(stormConf, nimbus_host, 6627); //thrift port

            //submit jar file with StormSubmitter to nimbus
            String uploadedJarLocation = StormSubmitter.submitJar(stormConf, inputJar);
            String jsonConf = JSONValue.toJSONString(stormConf);
            nimbusClient.getClient().submitTopology(topology_name, uploadedJarLocation, jsonConf, builder.createTopology());
        }
    }
}
