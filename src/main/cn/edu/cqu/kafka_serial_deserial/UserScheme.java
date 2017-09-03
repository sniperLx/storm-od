package cn.edu.cqu.kafka_serial_deserial;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import cn.edu.cqu.od.ODListNode;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;

/**
 * Created by lab on 2016/6/1.
 *
 * @author lx
 */

/**
 * UserScheme is a self-defined Kafka Spout's input scheme,
 * in which, it will deserialize data received from kafka_serial_deserial and
 * wrap into ODListNode.
 * It will also declare the outpout fields of KafkaSpout in method:
 * 'getOutputFields'.
 */
public class UserScheme implements Scheme {
    private static final long serialVersionUID = 938313245739159445L;

    public List<Object> deserialize(byte[] ser) {
        ByteArrayInputStream byteInputStream;
        ObjectInputStream objectInputStream;

        ODListNode odListNode;
        try {
            byteInputStream = new ByteArrayInputStream(ser);
            objectInputStream = new ObjectInputStream(byteInputStream);
            odListNode = (ODListNode) objectInputStream.readObject();
            return new Values(odListNode.getEid(), odListNode.getOdHashmap());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    public Fields getOutputFields() {
        return new Fields("eid", "single-car-od-map");
    }
}
