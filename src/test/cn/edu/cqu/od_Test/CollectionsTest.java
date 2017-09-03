package cn.edu.cqu.od_Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by lab on 2016/6/4.
 */
public class CollectionsTest {
    public static void main(String[] args) {
        List<? extends Number> foo3 = new ArrayList<Number>();
        Number a = 3;
        /**
         * You can't add any object to List<? extends T>
         * because you can't guarantee what kind of List it is really pointing to,
         * so you can't guarantee that the object is allowed in that List.
         * The only "guarantee" is that you can only
         * read from it and you'll get a T or subclass of  T.
         */
        //foo3.add(a); //error

        List<? super Number> foo4 = new ArrayList<Number>();
        /**
         * You can't read the specific type T (e.g. Number) from List<? super T>
         *     because you can't guarantee what kind of List it is really pointing to.
         *     The only "guarantee" you have is you are able to add a value of type T (or subclass of  T)
         *     without violating the integrity of the list being pointed to.
         */
        foo4.add(a);
    }
}
