package indi.zion.Mapreduces.MRJobSubmit;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MultiFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import indi.zion.Mapreduces.Maps.TopNMapperClass;
import indi.zion.Mapreduces.Maps.TopNMapperClass.RatingPerMovieMapper;
import indi.zion.Mapreduces.Maps.TopNMapperClass.TopNRatingMapper;
import indi.zion.Mapreduces.Reduces.TopNReduceClass;
import indi.zion.Mapreduces.Reduces.TopNReduceClass.RatingPerMovieReducer;
import indi.zion.Mapreduces.Reduces.TopNReduceClass.TopNMovieReducer;

public class TopNSubmit {

    private Job mappingjob;
    private Job topNjob;
    private Job joinJob;
    private Configuration conf;
    private Path rateInPath;
    private Path movieInPath;
    private Path mappingPath;
    private Path topNRatePath;
    private Path outPath;

    public void initJobs() throws IOException {
        conf = new Configuration();
        rateInPath = new Path("hdfs://hserver1:9000/user/root/dataRepo/mldata/ratings.dat");
        mappingPath = new Path("hdfs://hserver1:9000/user/root/dataRepo/mltmp/MappingRate");
        topNRatePath = new Path("hdfs://hserver1:9000/user/root/dataRepo/mltmp/TopNRate");
        movieInPath = new Path("hdfs://hserver1:9000/user/root/dataRepo/mldata/movies.dat");
        outPath = new Path("hdfs://hserver1:9000/user/root/dataRepo/mldata/Result.dat");
        
        mappingjob = Job.getInstance(conf, TopNSubmit.class.getSimpleName() + "Mappingjob");
        mappingjob.setJarByClass(TopNSubmit.class);
        FileInputFormat.setInputPaths(mappingjob, rateInPath);
        mappingjob.setMapperClass(TopNMapperClass.RatingPerMovieMapper.class);
        mappingjob.setReducerClass(TopNReduceClass.RatingPerMovieReducer.class);
        mappingjob.setMapOutputKeyClass(Text.class);
        mappingjob.setMapOutputValueClass(ArrayPrimitiveWritable.class);
        mappingjob.setOutputKeyClass(Text.class);
        mappingjob.setOutputValueClass(DoubleWritable.class);
        mappingjob.setInputFormatClass(TextInputFormat.class);
        mappingjob.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(mappingjob, mappingPath);

        topNjob = Job.getInstance(conf, TopNSubmit.class.getSimpleName() + "TopNjob");
        topNjob.setJarByClass(TopNSubmit.class);
        FileInputFormat.setInputPaths(topNjob, mappingPath);
        topNjob.setMapperClass(TopNMapperClass.TopNRatingMapper.class);
        topNjob.setReducerClass(TopNReduceClass.TopNMovieReducer.class);
        topNjob.setMapOutputKeyClass(DoubleWritable.class);
        topNjob.setMapOutputValueClass(Text.class);
        topNjob.setOutputKeyClass(Text.class);
        topNjob.setOutputValueClass(DoubleWritable.class);
        topNjob.setInputFormatClass(TextInputFormat.class);
        topNjob.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(topNjob, topNRatePath);
        
        joinJob = Job.getInstance(conf, TopNSubmit.class.getSimpleName() + "JoinJob");
        joinJob.setJarByClass(TopNSubmit.class);
        FileInputFormat.setInputPaths(joinJob, movieInPath, topNRatePath);
        joinJob.setMapperClass(TopNMapperClass.JoinMovieMapper.class);
        joinJob.setReducerClass(TopNReduceClass.JoinMovieReducer.class);
        joinJob.setMapOutputKeyClass(Text.class);
        joinJob.setMapOutputValueClass(Text.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);
        joinJob.setInputFormatClass(TextInputFormat.class);
        joinJob.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(joinJob, outPath);
    }

    public void MappingJob() {

        try {
            initJobs();
            System.out.println(mappingjob.waitForCompletion(true));
        } catch (ClassNotFoundException | IOException | InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void CascadeJobs() {
        
        try {
            initJobs();
            
            FileSystem mappingFs = FileSystem.get(mappingPath.toUri(), conf);
            FileSystem TopNFs = FileSystem.get(topNRatePath.toUri(), conf);
            FileSystem ResultFs = FileSystem.get(outPath.toUri(), conf);
            if (mappingFs != null) {
                mappingFs.delete(mappingPath, true);
            }
            if (TopNFs != null) {
                TopNFs.delete(topNRatePath, true);
            }
            if(ResultFs != null) {
                ResultFs.delete(outPath, true);
            }
            
            ControlledJob controlledMappingJob = new ControlledJob(mappingjob.getConfiguration());
            controlledMappingJob.setJob(mappingjob);
            ControlledJob controlledTopNJob = new ControlledJob(topNjob.getConfiguration());
            controlledTopNJob.setJob(topNjob);
            ControlledJob controlledJoinJob = new ControlledJob(joinJob.getConfiguration());
            controlledJoinJob.setJob(joinJob);
            
            controlledTopNJob.addDependingJob(controlledMappingJob);
            controlledJoinJob.addDependingJob(controlledTopNJob);
            
            JobControl controller = new JobControl("TopN Jobs");
            
            controller.addJob(controlledMappingJob);
            controller.addJob(controlledTopNJob);
            controller.addJob(controlledJoinJob);
            Thread job = new Thread(controller);
            job.start();

            while (true) {
                if (controller.getFailedJobList().size() > 0) {
                    System.out.println("failed jobs" + controller.getFailedJobList());
                    controller.stop();
                    mappingFs.delete(mappingPath, true);
                    mappingFs.delete(topNRatePath, true);
                    return;
                } else if (controller.allFinished()) {
                    System.out.println("success jobs:" + controller.getSuccessfulJobList());
                    controller.stop();
                    mappingFs.delete(mappingPath, true);
                    mappingFs.delete(topNRatePath, true);
                    return;
                } else {
                    Thread.sleep(500);
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

    }

}
