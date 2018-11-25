package indi.zion.Util;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

public class MovieDataCleanUtil extends DataCleanUtil {

    /*
     * Movies log data clean
     */
    @Override
    public String HiveLineFormat(Text str) {
        // spercial case for 7738::That's The Way I Like It (a.k.a. Forever Fever)
        // (1998)::Comedy|Drama|Romance
        String[] temp = str.toString().split("::");
        if (temp.length >= 3) {
            Pattern p = Pattern.compile("\\(\\d{4}\\)");
            String[] udc = new String[] { temp[0], "", "", temp[temp.length - 1] };
            Matcher m = p.matcher(temp[1]);
            if (m.find()) {
                udc[2] = m.group().substring(1, m.group().length() - 1);
            }
            udc[1] = p.split(temp[1])[0];
            StringBuffer dstr = new StringBuffer();
            for (String s : udc) {
                dstr.append(s).append("\t");
            }
            return dstr.substring(0, dstr.length() - 1).toString();
        }
        return str.toString().replaceAll("::", "\t");
    }

}
