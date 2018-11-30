package indi.zion.Util;

public class TempClassUtil {
    public static String SupplyClassName(String className) {
        if ("String".equals(className)) {
            return "java.lang.String";
        } else if ("int".equals(className)) {
            return "java.lang.Integer";
        }
        return "";
    }

    public static <V> V GetValue(byte[] b, Class<V> clz) {
        Object o = new Object();
        try {
            String className = clz.getName();
            if ("java.lang.String".equals(className)) {
                o = new String(b);
            }else if("java.lang.Integer".equals(className)) {
                o = (int) ((b[0] & 0xFF)   
                        | ((b[1] & 0xFF)<<8)   
                        | ((b[2] & 0xFF)<<16)   
                        | ((b[3] & 0xFF)<<24)); 
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
