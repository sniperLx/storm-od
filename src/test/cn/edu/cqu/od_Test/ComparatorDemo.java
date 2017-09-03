package cn.edu.cqu.od_Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Created by lab on 2016/4/29.
 */
public class ComparatorDemo {
    public static void main(String[] args) {
        ArrayList<Integer> demo = new ArrayList<Integer>();
        demo.add(12);
        demo.add(11);
        demo.add(5);
        demo.add(4);

        for (int i = 0; i < demo.size(); i++) {
            System.out.println(demo.get(i));
        }
        System.out.println("----------");
        Comparator<Integer> comparator = new Comparator<Integer>() {
            public int compare(Integer o1, Integer o2) {
                //����
                //return (o2-o1);

                //����
                return (o1 - o2);
            }
        };
        Collections.sort(demo, comparator);

        for (int i = 0; i < demo.size(); i++) {
            System.out.println(demo.get(i));
        }

        System.err.println(Integer.MIN_VALUE);
    }
}
