import org.json.JSONArray;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author maow
 * @create 2020-06-27 23:12
 */
public class Test1 {
    public static void main(String[] args) {

        Object[] o = new Object[3];
        //[{}]
        for (int i = 0; i<o.length;i++){
            o[i] = "{\"a"+i+"\":\"b\"}";
        }

        String jsonArray = o[0].toString();

        //{a:b,c:d}
        JSONArray actions = new JSONArray(jsonArray);
        System.out.println(actions);

//        for (int i = 0; i < actions.length(); i++) {
//
//            String[] result = new String[1];
//            result[0] = actions.getString(i);
//        }
    }
}
