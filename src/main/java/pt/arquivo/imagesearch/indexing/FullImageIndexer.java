package pt.arquivo.imagesearch.indexing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FullImageIndexer {

    public static void main(String[] args) throws Exception {
        assert args.length >= 1 : "Missing hdfs file with all arcs path argument";
        String hdfsArcsPath = args[0];

        assert args.length >= 2 : "Missing collection name argument";
        String collection = args[1];
        String jobName = collection + "_ImageIndexerWithDups";

        assert args.length >= 3 : "Missing number of warcs per map";
        int linesPerMap = Integer.parseInt(args[2]);

        assert args.length >= 4 : "Missing number of reduces";
        int reducesCount = Integer.parseInt(args[3]);


        Configuration conf = new Configuration();
        conf.set("collection", collection);

        Job job = Job.getInstance(conf);
        job.setJarByClass(FullImageIndexer.class);
        job.setInputFormatClass(NLineInputFormat.class);

        job.setMapperClass(ImageIndexerWithDups.Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ImageIndexerWithDups.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setJobName(jobName);

        NLineInputFormat.addInputPath(job, new Path(hdfsArcsPath));

        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/
        // Sets reducer tasks to 1
        job.setNumReduceTasks(reducesCount);
        //job.setNumReduceTasks(1);

        //job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/
        long jobTs = System.currentTimeMillis();
        String outputDirIntermediaryResults = "/user/amourao/output/" + collection + "/" + jobTs + "_dups";
        FileOutputFormat.setOutputPath(job, new Path(outputDirIntermediaryResults));

        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(new Path(outputDirIntermediaryResults)))
            hdfs.delete(new Path(outputDirIntermediaryResults), true);

        boolean result = job.waitForCompletion(true);


        System.out.println("ImageIndexerWithDups$IMAGE_COUNTERS");
        Counters cn = job.getCounters();
        CounterGroup counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDups$IMAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDups$PAGE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDups$PAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDups$REDUCE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDups$REDUCE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        if (!result){
            System.exit(1);
        }

        System.out.println("########################################################");

        jobName = collection + "_DupDigestMergerJob";
        Job jobDigest = Job.getInstance(conf);
        jobDigest.setJarByClass(DupDigestMergerJob.class);
        jobDigest.setInputFormatClass(KeyValueTextInputFormat.class);

        jobDigest.setMapperClass(DupDigestMergerJob.Map.class);
        jobDigest.setMapOutputKeyClass(Text.class);
        jobDigest.setMapOutputValueClass(Text.class);

        jobDigest.setReducerClass(DupDigestMergerJob.Reduce.class);
        jobDigest.setOutputKeyClass(NullWritable.class);
        jobDigest.setOutputValueClass(Text.class);
        jobDigest.setOutputFormatClass(TextOutputFormat.class);

        jobDigest.setJobName(jobName);

        jobDigest.setNumReduceTasks(reducesCount);

        String inputDirDigest = outputDirIntermediaryResults;

        KeyValueTextInputFormat.setInputDirRecursive(jobDigest, true);
        KeyValueTextInputFormat.addInputPath(jobDigest, new Path(inputDirDigest));

        String outputDirDigest = "/user/amourao/output/" + collection + "/" + jobTs + "_nodups/";
        TextOutputFormat.setOutputPath(jobDigest, new Path(outputDirDigest));
        if (hdfs.exists(new Path(outputDirDigest)))
            hdfs.delete(new Path(outputDirDigest), true);

        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        //job.setNumReduceTasks(1);

        //job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linespermap);
        //job.getConfiguration().setInt("mapreduce.job.running.map.limit", maxMaps); /*Maximum simultaneous maps running*/

        result = jobDigest.waitForCompletion(true);

        System.out.println("ImageIndexerWithDups$IMAGE_COUNTERS");
        cn = job.getCounters();
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDups$IMAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDups$PAGE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDups$PAGE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("ImageIndexerWithDups$REDUCE_COUNTERS");
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.ImageIndexerWithDups$REDUCE_COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        System.out.println("DupDigestMergerJob$COUNTERS");
        cn = jobDigest.getCounters();
        counterGroup = cn.getGroup("pt.arquivo.imagesearch.indexing.DupDigestMergerJob$COUNTERS");
        for (Counter c : counterGroup) {
            System.out.println("\t" + c.getName() + ": " + c.getValue());
        }

        if (hdfs.exists(new Path(outputDirIntermediaryResults)))
            hdfs.delete(new Path(outputDirIntermediaryResults), true);

        System.exit(result ? 0 : 1);
    }
}