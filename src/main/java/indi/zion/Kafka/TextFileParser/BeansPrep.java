package indi.zion.Kafka.TextFileParser;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Properties;

import indi.zion.Constant.StrSplitSign;
import indi.zion.InfoStream.Beans.Bean;
import indi.zion.InfoStream.Beans.Rate;
import indi.zion.Util.TempClassUtil;

/**
 * 
 * transform block to bean according to Temporary in .properties, by using
 * genericity class T invoke to Bean class and new instance. parser block with
 * line terminated by "\r|\n" and field terminated by ":"
 * 
 * @author Administrator
 * @param <T>
 */
public class BeansPrep<T extends Bean> {
    private Class<T> BeanClass;
    private String[][] ValueType;
    private Properties props = new Properties();

    public BeansPrep(Class<T> classType) {
        this.BeanClass = classType;
        InitBeanStruct();
    }

    public ArrayList<T> ToBeans(byte[] Block) {
        ArrayList<T> ware = new ArrayList<T>();
        try {
            Constructor<T> cons = BeanClass.getConstructor();
            T bean = cons.newInstance();
            for (int i = 0, beanStart = 0, beanEnd = 0, no = 0; i < Block.length; i++) {
                if (Block[i] == StrSplitSign.ls && Block[i + 1] == StrSplitSign.ls) {
                    beanEnd = i;
                    Setter(bean, ValueType, no, Block, beanStart, beanEnd);
                    while ((Block[i] == StrSplitSign.n || Block[i] == StrSplitSign.r || Block[i] == StrSplitSign.ls)) {
                        i++;//pass when byte belong sign of split
                        if(i == Block.length)break;
                    }
                    beanStart = i;
                    no += 1;
                }
                if (Block[i] == StrSplitSign.n || Block[i] == StrSplitSign.r || Block[i] == StrSplitSign.end) {
                    beanEnd = i;
                    if (beanEnd != beanStart) {
                        Setter(bean, ValueType, no, Block, beanStart, beanEnd);
                        ware.add(bean);
                        bean = cons.newInstance();// as store into ware, then new instance
                    }
                    while ((Block[i] == StrSplitSign.n || Block[i] == StrSplitSign.r || Block[i] == StrSplitSign.ls)) {
                        i++;//pass when byte belong sign of split
                        if(i == Block.length)break;
                    }
                    beanStart = i;
                    no = 0;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ware;
    }

    /**
     * catch the byte set to one of value in beans according the temporary of
     * .properties
     * 
     * @throws Exception
     */
    private void Setter(T bean, String[][] ValueType, int no, byte[] Block, int beanStart, int beanEnd)
            throws Exception {
        byte[] tempValue = new byte[beanEnd - beanStart];
        System.arraycopy(Block, beanStart, tempValue, 0, beanEnd - beanStart);
        Class<?> tempType = Class.forName(TempClassUtil.SupplyClassName(ValueType[no][1]));
        BeanClass.getMethod("set" + ValueType[no][0], tempType).invoke(bean,
                TempClassUtil.GetValue(tempValue, tempType));
    }

    private void InitProp() {
        try {
            InputStream inStream = TextReader.class.getResourceAsStream("MsgStructure.properties");
            props.load(inStream);
            inStream.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private void InitBeanStruct() {
        InitProp();
        System.out.println(BeanClass.getName());
        String BeanClassName = BeanClass.getName();// package + class name
        String[] Values = props.getProperty(BeanClassName.substring(BeanClassName.lastIndexOf(".") + 1)).split(",");
        ValueType = new String[Values.length][2];
        int i = 0;
        for (String value : Values) {
            ValueType[i++] = value.split(":");
        }
    }
}
