import java.util.HashMap;
import java.util.Map;

/**
 * Created by lab on 2016/6/1.
 */
public class getODAlgrithomTest {
    public static Integer[] a = new Integer[]{6, 7, 8, 10, 11};
    public static HashMap<Integer, Integer> map = new HashMap<Integer, Integer>();

    public static void main(String[] args) {
        Integer cur_O = null;
        Integer cur_D = null;

        Integer temp_O;
        Integer temp_D;
        for (int i = 1; i < a.length; i++) {
            temp_O = a[i - 1];
            temp_D = a[i];

            if (i == 1) {
                cur_O = temp_O;
            }

            if (temp_D - temp_O > 1) {
                cur_D = temp_O;
                if (!cur_D.equals(cur_O)) {
                    map.put(cur_O, cur_D);
                }
                cur_O = temp_D;
            }

            if (i == a.length - 1) {
                cur_D = temp_D;
                if (!cur_D.equals(cur_O)) {
                    map.put(cur_O, cur_D);
                }
            }
        }

        for (Map.Entry<Integer, Integer> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " ===> " + entry.getValue());
        }
    }
}
