package indi.zion.Mapreduces.Execute;

import org.apache.log4j.Logger;

//import org.apache.log4j.Logger;

import indi.zion.Mapreduces.MRJobSubmit.TopNSubmit;

/**
 * Hello world!
 *
 */
public class App {

    private static Logger log = Logger.getLogger(App.class);

    public static void main(String[] args) {
        // Movie Rate TopN
        new Thread(() -> {
            new TopNSubmit().CascadeJobs();
        }).start();

    }
}
