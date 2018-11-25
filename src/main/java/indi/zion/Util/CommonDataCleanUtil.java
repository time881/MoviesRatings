package indi.zion.Util;

import org.apache.hadoop.io.Text;

public class CommonDataCleanUtil extends DataCleanUtil {

    @Override
    /*
     * Tags and ratings log data clean
     */
    public String HiveLineFormat(Text str) {
        // TODO Auto-generated method stub
        return str.toString().replaceAll("::", "\t");
    }

}
