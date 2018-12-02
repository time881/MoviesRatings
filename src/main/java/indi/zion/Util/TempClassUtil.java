package indi.zion.Util;

public class TempClassUtil {
    public static String SupplyClassName(String className) {
        if ("String".equals(className)) {
            return "java.lang.String";
        } else if ("int".equals(className)) {
            return "java.lang.Integer";
        }else if ("double".equals(className)) {
            return "java.lang.Double";
        }
        return "";
    }

    public static <V> V GetValue(byte[] b, Class<V> clz) {
        Object o = new Object();
        try {
            String className = clz.getName();
            String str = new String(b);
            if ("java.lang.String".equals(className)) {
                o = str;
            }else if("java.lang.Integer".equals(className)) {
                o = Integer.valueOf(str); 
            }else if("java.lang.Double".equals(className)) {
                o = Double.valueOf(str);
            }
            if (clz.isInstance(o)) {
                return clz.cast(o);
            }
            return (V) o;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
