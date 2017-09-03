package cn.edu.cqu.od_drpc;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import cn.edu.cqu.od.GetEIDNoKafkaSpout;
import cn.edu.cqu.od.GetPathChainBolt;
import cn.edu.cqu.od.ODCalcBolt;
import cn.edu.cqu.od.SavaODBolt;

/**
 * Created by lab on 2016/4/25.
 *
 * @author lx
 */
public class TopologyLocalDRPC {
    public static void main(String[] args) throws Exception {
        //define topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("get-eid", new GetEIDNoKafkaSpout(), 1);
        builder.setBolt("get-path-chain-bolt", new GetPathChainBolt(), 3)
                .shuffleGrouping("get-eid");
        builder.setBolt("calc-single-car-od", new ODCalcBolt(), 4)
                .shuffleGrouping("get-path-chain-bolt");
        builder.setBolt("save-od", new SavaODBolt(), 4)
                .shuffleGrouping("calc-single-car-od");

        //topology configure
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumAckers(2);
        //conf.setMaxTaskParallelism(5);

        //define a localcluster
        LocalCluster cluster = new LocalCluster();

        //submit topology
        String topology_name = "od-matrix1";
        cluster.submitTopology(topology_name, conf, builder.createTopology());


        //cluster.killTopology(topology_name);
        //Thread.sleep(200000);
        //cluster.shutdown();

    }
}
