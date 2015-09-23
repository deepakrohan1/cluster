import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * @author Deepak Rohan Sekar
 * @date 23 September 2015
 * @version 1.0
 * @file TermFrequency.java
 */

public class TermFrequency extends Configured implements Tool {

    private static final Logger LOG = Logger .getLogger( DocWordCount.class);

    public static void main( String[] args) throws  Exception {
        int res  = ToolRunner .run( new TermFrequency(), args);
        System .exit(res);
    }

    public int run( String[] args) throws  Exception {
        Job job  = Job .getInstance(getConf(), " wordcount ");
        job.setJarByClass( this .getClass());

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
        job.setMapperClass( Map .class);
        job.setReducerClass( Reduce .class);
        job.setOutputKeyClass( Text .class);
        job.setOutputValueClass( IntWritable .class);

        return job.waitForCompletion( true)  ? 0 : 1;
    }


    public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
        private final static IntWritable one  = new IntWritable( 1);
        private Text word  = new Text();

        private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

        public void map( LongWritable offset,  Text lineText,  Context context)
                throws  IOException,  InterruptedException {

            String line  = lineText.toString();
            Text currentWord  = new Text();
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
//            System.out.println(fileName);
//         FileSplit fileSplit = (FileSplit)context.getInputSplit();
//         String filename1 = fileSplit.getPath().getName();

            for ( String word  : WORD_BOUNDARY .split(line)) {
                if (word.isEmpty()) {
                    continue;
                }

                currentWord  = new Text(word+"####"+fileName);
                context.write(currentWord,one);
            }
        }
    }

    public static class Reduce extends Reducer<Text ,  IntWritable ,  Text , DoubleWritable> {
        @Override
        public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
                throws IOException,  InterruptedException {
            int sum  = 0;
            double logValues =0.0;
            for ( IntWritable count  : counts) {
                sum  += count.get();
                if(sum == 0){
                    logValues = 0.0;
                }else {
                    logValues = 1 + Math.log10(sum);
                    System.out.println(logValues);
                }
            }
            context.write(word,  new DoubleWritable(logValues));
        }
    }
}
