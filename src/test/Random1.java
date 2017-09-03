import java.util.ArrayList;
import java.util.List;

public class Random1 {
    public static void main(String args[]) {
        int a[] = new int[10];
        for (int i = 0; i < 10; i++) {
            a[i] = (int) (Math.random() * 10);
            //这个地方不能写成(int)Math.random()这是最大的错误
            //其他地方乱七八糟的，我就自己写了
        }
        for (int i = 0; i < 10; )
            System.out.println(i + " : " + a[i++]);//代码中最好不要出现中文

        List<Integer> test = new ArrayList<Integer>();
    }
}