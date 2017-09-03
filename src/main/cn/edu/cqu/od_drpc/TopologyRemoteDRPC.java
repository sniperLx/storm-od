package cn.edu.cqu.od_drpc;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;
import backtype.storm.generated.Nimbus;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import cn.edu.cqu.od.GetPathChainBolt;
import cn.edu.cqu.od.ODCalcBolt;
import cn.edu.cqu.od.SavaODBolt;
import org.json.simple.JSONValue;

import java.util.Map;

/**
 * Created by lab on 2016/4/25.
 *
 * @author lx
 */
public class TopologyRemoteDRPC {
    public static void main(String[] args) throws Exception {

        //define topology
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("od-drpc");
        builder.addBolt(new GetPathChainBolt(), 7);
        builder.addBolt(new ODCalcBolt(), 6)
                .shuffleGrouping();
        builder.addBolt(new SavaODBolt(), 5)
                .shuffleGrouping();

        //define conf of topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(6);

        if (args.length > 0) {
            StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
        } else {
            //read Storm configuration
            String nimbus_host = "node1_IP";
            Map<String, Object> stormConf = Utils.readStormConfig();
            stormConf.put("nimbus.host", nimbus_host);
            stormConf.putAll(conf);

            Nimbus.Client client = NimbusClient.getConfiguredClient(stormConf).getClient();
            String inputJar = "E:\\Documents\\IdeaProjects\\storm-od\\target\\storm-od-1.0-SNAPSHOT-jar-with-dependencies.jar";
            NimbusClient nimbusClient = new NimbusClient(stormConf, nimbus_host, 6627);

            //submit StormSubmitter jar
            String uploadedJarLocation = StormSubmitter.submitJar(stormConf, inputJar);
            String jsonConf = JSONValue.toJSONString(stormConf);
            String topology_name = "od-drpc";
            nimbusClient.getClient().submitTopology(topology_name, uploadedJarLocation, jsonConf, builder.createRemoteTopology());
        }
    }

}
