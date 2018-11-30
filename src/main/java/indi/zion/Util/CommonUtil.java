package indi.zion.Util;

import indi.zion.Constant.SpaceUnit;

public class CommonUtil {
    public static Double FormatUnit(String Num) {
        int len = Num.length();
        for (int i = len - 1; i >= 0; i--) {
            if (Num.charAt(i) >= 48 && Num.charAt(i) <= 57) {
                String Unit = Num.substring(i + 1);
                double value = Double.valueOf(Num.substring(0, i + 1));
                if (SpaceUnit.K.equals(Unit)) {
                    return value * 1024.0;
                } else if (SpaceUnit.M.equals(Unit)) {
                    return value * 1024.0 * 1024.0;
                } else if (SpaceUnit.B.equals(Unit)) {
                    return value;
                } else if (SpaceUnit.G.equals(Unit)) {
                    return value * 1024.0 * 1024.0 * 1024.0;
                }
            }
        }
        return 0.0;
    }
}
