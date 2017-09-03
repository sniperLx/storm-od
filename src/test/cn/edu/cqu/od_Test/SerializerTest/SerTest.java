package cn.edu.cqu.od_Test.SerializerTest;

import org.junit.Assert;
import org.junit.Test;

import java.io.*;

/**
 * Created by lab on 2016/6/1.
 *
 * @author lx
 */
public class SerTest {
    @Test
    public void serializeToDisk() {
        try {
            Person ted = new Person("Ted", "Neward", 39);
            Person charl = new Person("Charlotte",
                    "Neward", 38);

            ted.setSpouse(charl);
            charl.setSpouse(ted);

            FileOutputStream fos = new FileOutputStream("tempdata.ser");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(ted);
            oos.close();
        } catch (Exception ex) {
            System.err.println("Exception thrown during test: " + ex.toString());
        }

        try {
            FileInputStream fis = new FileInputStream("tempdata.ser");
            ObjectInputStream ois = new ObjectInputStream(fis);
            Person ted = (Person) ois.readObject();
            ois.close();
            Assert.assertEquals(ted.getFirstName(), "Ted");
            Assert.assertEquals(ted.getSpouse().getFirstName(), "Charlotte");

            // Clean up the file
            new File("tempdata.ser").delete();
        } catch (Exception ex) {
            System.err.println("Exception thrown during test: " + ex.toString());
        }
    }
}
