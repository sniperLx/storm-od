package cn.edu.cqu.od;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by lab on 2016/4/26.
 *
 * @author lx
 */
//calculate single car's OD
public class ODCalcBolt extends BaseRichBolt {
    private static final long serialVersionUID = 976863707798034212L;

    private static final Logger LOG = LoggerFactory.getLogger(ODCalcBolt.class);
    private OutputCollector _collector;

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("eid", "single-car-od-list"));
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }

    public void execute(Tuple tuple) {
        ArrayList<PathInfoNode> pathChain;
        HashMap<PathInfoNode, PathInfoNode> odHashmap;

        String eid = tuple.getStringByField("eid");
        Object value = tuple.getValueByField("single-car-path-chain");

        if (value instanceof ArrayList) {
            pathChain = (ArrayList<PathInfoNode>) value;
            odHashmap = getSingleCarOD(pathChain);
            ODListNode odListNode = new ODListNode(eid, odHashmap);

            _collector.emit(tuple, new Values(eid, odListNode));
            _collector.ack(tuple);
        } else {
            LOG.error("storm received type of value of tuple isn't List");
        }
    }

    /*
     * Here calculate the OD map of the given car's pathChain
     */
    private HashMap<PathInfoNode, PathInfoNode> getSingleCarOD(ArrayList<PathInfoNode> pathChain) {

        //the key is 'O' and the value is 'D'
        HashMap<PathInfoNode, PathInfoNode> singleCarOD = new HashMap<PathInfoNode, PathInfoNode>();
        PathInfoNode cur_O = null;
        PathInfoNode cur_D;
        PathInfoNode temp_O;
        PathInfoNode temp_D;
        for (int i = 1; i < pathChain.size(); i++) {
            temp_O = pathChain.get(i - 1);
            temp_D = pathChain.get(i);

            /**
             * If it's the first node of pathChain,
             * set it as 'O' of a OD.
             */
            if (i == 1) {
                cur_O = temp_O;
                cur_O.setFlag(FLAG.ORIGINATION);
            }
            /**
             * The time_threshold is 3600s. When the interval between two rfid_reader is larger than 3600s,
             * previous reader will be D in last od, and next reader will be O in next od.
             * For instance, when time interval between two consecutive readers 'A' and 'B' goes beyond 3600s,
             * set reader 'A' as last 'D', and set reader 'B' as new 'O'
             */
            long time_interval = (temp_D.getPassTime().getTime() - temp_O.getPassTime().getTime()) / 1000;
            if (time_interval > 3600) {
                /**
                 * exclude self od, in which 'O' is the same reader as 'D'.
                 */
                cur_D = temp_O;
                cur_D.setFlag(FLAG.DESTINATION);
                if (!cur_D.equals(cur_O)) {
                    /**
                     * Save previous od into hashmap singleCarOD.
                     * cur_O is 'O' and cur_D is 'D'
                     */
                    singleCarOD.put(cur_O, cur_D);
                }
                cur_O = temp_D;
                cur_O.setFlag(FLAG.ORIGINATION);
            }

            if (i == pathChain.size() - 1) {
                cur_D = temp_D;
                cur_D.setFlag(FLAG.DESTINATION);
                if (cur_D != cur_O) {
                    singleCarOD.put(cur_O, cur_D);
                }
            }
        }
        return singleCarOD;
    }

}
