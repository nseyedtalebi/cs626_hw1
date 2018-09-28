package org.nms.cs626;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.nms.cs626.util.OrderedPair;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.util.List;
import java.util.stream.StreamSupport;


public class App extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(App.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new App(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "hw1_part1");
        job.setNumReduceTasks(1);
        job.setJarByClass(this.getClass());
        // Use TextInputFormat, the default unless job.setInputFormatClass is used
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        boolean compl = job.waitForCompletion(true);
        Long pairs  = job.getCounters().findCounter(Reduce.CountersEnum.UNIQUE_OUTPUT_PAIRS).getValue();
        System.out.println("Unique output pairs: "+pairs.toString());
        return compl ? 0 : 1;
    }

    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map  (LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            List<CSVRecord> records =  CSVParser
                    .parse(lineText.toString(),CSVFormat.DEFAULT)
                    .getRecords();
            OrderedPair keyPair = new OrderedPair(records.get(0).get(0), records.get(0).get(1));
            if(!OrderedPair.LexicalLessOrEqual(keyPair.left,keyPair.right)){
                keyPair = keyPair.reverse();
            }
            System.out.println(keyPair);
            context.write(keyPair.asText(),new IntWritable(1));
        }
    }

    public static class Reduce extends Reducer<Text
            ,IntWritable
            ,Text
            ,IntWritable>{
        enum CountersEnum {UNIQUE_OUTPUT_PAIRS}

        public void reduce(Text keyin, Iterable<IntWritable> valueIn,
                           Context context) throws IOException, InterruptedException {
            context.getCounter(CountersEnum.UNIQUE_OUTPUT_PAIRS).increment(1);
            Integer theSum = StreamSupport.stream(valueIn.spliterator(),false).map(IntWritable::get).reduce(0,(a,b)->a+b);
            context.write(keyin,new IntWritable(theSum));
        }
    }
}
