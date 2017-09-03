package cn.edu.cqu.od_Test.SerializerTest;

import cn.edu.cqu.kafka_serial_deserial.UserEncoder;
import cn.edu.cqu.od.FLAG;
import cn.edu.cqu.od.ODListNode;
import cn.edu.cqu.od.PathInfoNode;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

/**
 * Created by lab on 2016/6/1.
 *
 * @author lx
 */
public class UserEncoderTest {
    @Test
    public void UserEncoderSer() {
        UserEncoder userEncoder = new UserEncoder();
        HashMap<PathInfoNode, PathInfoNode> odHashmap = new HashMap<PathInfoNode, PathInfoNode>();

        //key
        PathInfoNode origin = new PathInfoNode();
        origin.setFlag(FLAG.ORIGINATION);
        origin.setIp("10.10.2.12");
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = sdf.parse("2016-02-29 01:24:45");
            origin.setPassTime(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //value
        PathInfoNode dest = new PathInfoNode();
        dest.setFlag(FLAG.DESTINATION);
        dest.setIp("10.10.2.25");
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = sdf1.parse("2016-02-29 02:00:45");
            dest.setPassTime(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        odHashmap.put(origin, dest);

        String eid = "10000";
//        ODListNode odListNode = new ODListNode();
//        odListNode.setEid(eid);
//        odListNode.setOdHashmap(odHashmap);
        ODListNode odListNode = new ODListNode(eid, odHashmap);

        //serialize
        byte[] bytes = userEncoder.toBytes(odListNode);
        System.out.println("==========serialized result===============");
        for (byte b : bytes) {
            System.out.print(b + " ");
        }

        //deserialize
        ODListNode deser = userEncoder.fromBytes(bytes);
        System.out.println("\n============deserialized result=============================");
        System.out.println(deser);
    }
}
