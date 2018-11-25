package Test.Zion.Util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import indi.zion.InfoStream.ReadTextUtil;

public class AllTests {

    
    public void HiveLineFormat() {
        Text str = // new Text();
                new Text("7738::That's The Way I Like It (a.k.a. Forever Fever) (1998)::Comedy|Drama|Romance");
        String[] temp = str.toString().split("::");
        if (temp.length == 3) {
            Pattern p = Pattern.compile("\\(\\d{4}\\)");
            String[] udc = new String[] { temp[0], "", "", temp[temp.length - 1] };
            Matcher m = p.matcher(temp[1]);
            if (m.find()) {
                udc[2] = m.group().substring(1, m.group().length() - 1);
            }
            udc[1] = p.split(temp[1])[0];
            StringBuffer dstr = new StringBuffer();
            for (String s : udc) {
                dstr.append(s);
                dstr.append("~");
            }
            // return new Text(dstr.substring(0, dstr.length()-1).toString());
            assertThat(dstr.substring(0, dstr.length() - 1).toString(),is("7738~That's The Way I Like It (a.k.a. Forever Fever) ~1998~Comedy|Drama|Romance"));
        } else
            assertThat(str.toString().replaceAll("::", "~"), is(""));
    }
    
   
    public void SubIndexof() {
        String A = "7123::Naked Lunch (1991)::Drama|Fantasy|Mystery|Sci-Fi";
        System.out.println(A.substring(0, A.indexOf(":")));
        System.out.println("02\t"+A.substring(A.indexOf(":")+2).replaceAll("::", "\t"));
        
        String line = "7823\t4.5";
        System.out.println(line.substring(0, line.indexOf("\t")));
        System.out.println("01\t"+line.substring(line.indexOf("\t")+1));
        
        String str = "7823\t01\t4.5";
        System.out.println(str.substring(str.indexOf("\t")+1,str.indexOf("\t")+3));
        
        String text = "7123\t02\tNaked Lunch (1991)\tDrama|Fantasy|Mystery|Sci-Fi";
        int t = text.indexOf("\t");
        System.out.println(text.substring(0,2));
        
    }
    
    @Test
    public void StreamInfo() {
        String A = "我\r\n是a:bcABC\n";
        byte[] B = A.getBytes();
        byte[] C = new byte[] {97,10,98};
        changes(C);
        System.out.println(new String(C));
        
        for(byte b : B) {
            System.out.println(b);
        }
        
    }
    
    public void changes(byte[] C) {
        C[1] = 0;
    }

}
