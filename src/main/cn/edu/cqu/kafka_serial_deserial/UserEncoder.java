package cn.edu.cqu.kafka_serial_deserial;

import cn.edu.cqu.od.ODListNode;
import kafka.serializer.Decoder;
import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by lab on 2016/5/31.
 *
 * @author lx
 */
public class UserEncoder implements Encoder<ODListNode>, Decoder<ODListNode> {
    public static final Logger LOG = LoggerFactory.getLogger(UserEncoder.class);

    public UserEncoder() {
    }

    //This method is necessary,according to the definition of Encoder.
    public UserEncoder(VerifiableProperties props) {
    }

    public byte[] toBytes(ODListNode odListNode) {
        //LOG.info("encoding start ...");
        ObjectOutputStream oos;
        ByteArrayOutputStream baos;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(odListNode);
            byte[] bytes = baos.toByteArray();
            oos.flush();
            oos.close();
            return bytes;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public ODListNode fromBytes(byte[] bytes) {
        ByteArrayInputStream byteInputStream;
        ObjectInputStream objectInputStream;

        ODListNode odListNode;
        try {
            byteInputStream = new ByteArrayInputStream(bytes);
            objectInputStream = new ObjectInputStream(byteInputStream);
            odListNode = (ODListNode) objectInputStream.readObject();
            return odListNode;
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }
}
