package cn.edu.cqu.od_Test;

/**
 * Created by lab on 2016/5/23.
 */
public class UtilsTest {
    public static void main(String[] args) {
        String string = "hello, world";
        int off = string.indexOf('o');
        int last_off = string.lastIndexOf('o');
        System.out.format("off: %d  last_off: %d\n", off, last_off);
        String substring = string.substring(off);
        String substring1 = string.substring(0, off);
        System.out.println("substring: " + substring);
        System.out.println("substring1: " + substring1);
    }
}
