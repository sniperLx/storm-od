package cn.edu.cqu.od_Test;

import backtype.storm.utils.Utils;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by lab on 2016/4/29.
 */
public class BlockingListDemo {
    //click psvm will auto produce
    public static void main(String[] args) {
        final BlockingDeque<Integer> blockingDeque = new LinkedBlockingDeque<Integer>(10);

        Thread producer = new Thread(new Runnable() {
            public void run() {
                int i = 0;
                while (i++ < 100) {
                    try {
                        blockingDeque.put(i);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        Thread consumer1 = new Thread(new Runnable() {
            public void run() {
                int i = 0;
                while (i++ < 100) {
                    Integer data = blockingDeque.poll();
                    System.err.println("in consumer1: " + data);
                    Utils.sleep(1);
                }
            }
        });

        Thread consumer2 = new Thread(new Runnable() {
            public void run() {
                int i = 0;
                while (i++ < 100) {
                    Integer data = blockingDeque.poll();
                    System.err.println("in consumer2: " + data);
                    Utils.sleep(1);
                }
            }
        });


        producer.start();
        consumer1.start();
        consumer2.start();

        try {
            producer.join();
            consumer1.join();
            consumer2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
