package Test.Zion.Util;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import indi.zion.Kafka.TextFileParser.TextReader;

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
            assertThat(dstr.substring(0, dstr.length() - 1).toString(),
                    is("7738~That's The Way I Like It (a.k.a. Forever Fever) ~1998~Comedy|Drama|Romance"));
        } else
            assertThat(str.toString().replaceAll("::", "~"), is(""));
    }

    public void SubIndexof() {
        String A = "7123::Naked Lunch (1991)::Drama|Fantasy|Mystery|Sci-Fi";
        System.out.println(A.substring(0, A.indexOf(":")));
        System.out.println("02\t" + A.substring(A.indexOf(":") + 2).replaceAll("::", "\t"));

        String line = "7823\t4.5";
        System.out.println(line.substring(0, line.indexOf("\t")));
        System.out.println("01\t" + line.substring(line.indexOf("\t") + 1));

        String str = "7823\t01\t4.5";
        System.out.println(str.substring(str.indexOf("\t") + 1, str.indexOf("\t") + 3));

        String text = "7123\t02\tNaked Lunch (1991)\tDrama|Fantasy|Mystery|Sci-Fi";
        int t = text.indexOf("\t");
        System.out.println(text.substring(0, 2));

    }

    public void StreamInfo() {
        String A = "1234a";
        byte[] B = A.getBytes();
        byte[] C = new byte[] { 97, 10, 98 };
        byte[] D = new byte[3];
        System.arraycopy(C, 1, D, 0, 2);
        System.out.println(A);

        for (byte b : B) {
            System.out.println(b);
        }

        try {
            System.out.println(String.class.getName());
            Class<?> c = Class.forName("java.lang.Integer");
            String a = "1";
            System.out.println(get(c, A));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public static int bytesToInt(byte[] src, int offset) {
        int value;
        value = (int) ((src[offset] & 0xFF) | ((src[offset + 1] & 0xFF) << 8) | ((src[offset + 2] & 0xFF) << 16)
                | ((src[offset + 3] & 0xFF) << 24));
        return value;
    }

    public static <T> T get(Class<T> clz, Object o) {
        if (clz.isInstance(o)) {
            return clz.cast(o);
        }
        return (T) o;
    }

    public void changes(byte[] C) {
        C[1] = 0;
    }

    public void TestInvok() {
        Object o = 1;
        System.out.println(Integer.class.isInstance(o));
    }

    public class ThreadPoolTest2 implements Runnable {
        int i;

        public ThreadPoolTest2(int i) {
            this.i = i;
        }

        public void run() {
            synchronized (this) {
                try {
                    System.out.println("线程名称：" + Thread.currentThread().getName() + "-" + i);
                    Thread.sleep(30); // 休眠是为了让该线程不至于执行完毕后从线程池里释放
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Test
    public void ThreadPool() {
        try {
            BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>(4); // 固定为4的线程队列
            RejectedExecutionHandler re = new RejectedExecutionHandler() {
                @Override
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    if (!executor.isShutdown()) {
                        try {
                            executor.getQueue().put(r);
                        } catch (InterruptedException e) {
                            // should not be interrupted
                        }
                    }
                }
            };

            ThreadPoolExecutor executor = new ThreadPoolExecutor(2, 2, 1, TimeUnit.DAYS, queue, re);
            for (int i = 0; i < 6; i++) {
                executor.submit(new Thread(new ThreadPoolTest2(i)));
                int threadSize = queue.size();
                System.out.println("线程队列大小为-->" + threadSize);
            }
            synchronized (this) {
                this.wait();
            }
            executor.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
