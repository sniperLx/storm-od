package cn.edu.cqu.topology;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Nimbus;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import cn.edu.cqu.od.*;
import org.json.simple.JSONValue;

import java.util.Map;

/**
 * Created by lab on 2016/4/25.
 *
 * @author lx
 */
public class TopologyRemote {
    public static void main(String[] args) throws Exception {
        //define topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("get-eid", new GetEIDNoKafkaSpout(), 1);
        builder.setBolt("get-path-chain-bolt", new GetPathChainBolt(), 7)
                .shuffleGrouping("get-eid");
        builder.setBolt("save-onpEID-bolt", new SaveONPEIDNoKafkaBolt(), 2)
                .directGrouping("get-path-chain-bolt", "oneNodePath");
        builder.setBolt("calc-single-car-od", new ODCalcBolt(), 6)
                .shuffleGrouping("get-path-chain-bolt");
        builder.setBolt("save-od", new SaveODNoKafkaBolt(), 5)
                .shuffleGrouping("calc-single-car-od");

        //define conf of topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(7);
        conf.setNumAckers(2);
        conf.setMaxSpoutPending(300);
        conf.setMessageTimeoutSecs(30);

        if (args != null && args.length > 0) {
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            //读取本地 Storm 配置文件
            String nimbus_host = "node1_IP";
            Map<String, Object> stormConf = Utils.readStormConfig();
            stormConf.put("nimbus.host", nimbus_host);
            stormConf.putAll(conf);

            Nimbus.Client client = NimbusClient.getConfiguredClient(stormConf).getClient();
            String inputJar = ".\\target\\storm-od-1.0-SNAPSHOT-jar-with-dependencies.jar";
            NimbusClient nimbusClient = new NimbusClient(stormConf, nimbus_host, 6627);

            //使用StormSubmitter提交jar包
            String uploadedJarLocation = StormSubmitter.submitJar(stormConf, inputJar);
            String jsonConf = JSONValue.toJSONString(stormConf);
            String topology_name = "od-calc-remote";
            nimbusClient.getClient().submitTopology(topology_name, uploadedJarLocation, jsonConf, builder.createTopology());
        }
    }

}
