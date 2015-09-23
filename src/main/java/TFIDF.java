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
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author Deepak Rohan Sekar
 * @date 23 September 2015
 * @version 1.0
 * @file TFIDF.java
 */

public class TFIDF extends Configured implements Tool {
    static String key;
    static String value;
    static String value1;
    static String value2;
    static String[] valuepairs;
    static HashSet<String> files = new HashSet<String>();


    private static final Logger LOG = Logger.getLogger(DocWordCount.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TFIDF(), args);
        System.exit(res);
    }

    /**
     *
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " wordcount ");
        job.setJarByClass(this.getClass());
        Job secondJob = Job.getInstance(getConf(), " tfidf ");
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[0]+"/temp"));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("Finishing First");
        if (job.waitForCompletion(true)) {

            secondJob.setJarByClass(this.getClass());
            FileInputFormat.addInputPaths(secondJob, args[0]+"/temp");
            FileOutputFormat.setOutputPath(secondJob, new Path(args[1]));
            secondJob.setMapperClass(Map2.class);
            secondJob.setReducerClass(Reduce2.class);
            secondJob.setOutputKeyClass(Text.class);
            secondJob.setOutputValueClass(Text.class);
        }
        return secondJob.waitForCompletion(true) ? 0 : 1;
    }

    /**
     *First Mapper Job to get the files in the input folder and split the words based on the pattern
     * Once the split has been made each word is appended with #### and fileName and value of 1
     * <Hadoop####file0, 1>Key value pairs of the mapper
     */
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            Text currentWord = new Text();
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
            files.add(fileName);
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }

                currentWord = new Text(word + "####" + fileName);
                context.write(currentWord, one);
            }
        }
    }

    /**
     * The reducer uses the iterable to add up the occurances of the word in different input documents
     * Calculation of the logarthimic frequency for the summed up values
     */
    public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        @Override
        public void reduce(Text word, Iterable<IntWritable> counts, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            double logValues = 0.0;

            for (IntWritable count : counts) {
                sum += count.get();
                if (sum == 0) {
                    logValues = 0.0;
                } else {
                    logValues = 1 + Math.log10(sum);
                }
            }
            context.write(word, new DoubleWritable(logValues));
        }
    }

    /**
     * Map2 is another mapper job which uses the string tokenizer to split the input <Hadoop####file0,1.0>
     * Once the split is done the word is sent as the key and value is the file name and frequency as <Hadoop,file0=1.0>
     * The text ouput is then passed to the Reducer2 for final phase.
     */
    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s");

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {
            String line = lineText.toString();
            Text currentWord = new Text();
            Text valuePair = new Text();
            StringTokenizer st = new StringTokenizer(line, "####");
            int i = 0;
            while (st.hasMoreTokens()) {
                if (i == 0) {
                    key = st.nextToken();
//                        System.out.println("Key, "+key);

                } else if (i == 1) {
                    value = st.nextToken();
                    StringTokenizer st1 = new StringTokenizer(value, "\t");
                    int k = 0;
                    while (st1.hasMoreTokens()) {
                        if (k == 0) {
                            value1 = st1.nextToken();
                        } else if (k == 1) {
                            value2 = st1.nextToken();
                        }
                        k++;
                    }
                }

                i++;
            }
            currentWord = new Text(key);
            valuePair = new Text(value1 + "=" + value2);
            context.write(currentWord, valuePair);
        }
    }

    /**
     * Reduce 2 is used to calculate the IDF and TFIDF for a given word
     * The output is Text which has the word and DoubleWritable which has the TDIDF
     * The format of the output is  <yellow####, TDIDF>
     */
    public static class Reduce2 extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text word, Iterable<Text> texts, Context context)
                throws IOException, InterruptedException {
            HashMap<String, String> fileSet = new HashMap<String,String>();

            int i =0;
            Double iDF = 0.00;
            Double tFiDF = 0.00;

            Text wordFiles = new Text();


            for (Text text : texts) {
                valuepairs = text.toString().split("=");
                i++;

                fileSet.put(valuepairs[0].toString(), valuepairs[1].toString());
                }
                for (String key : fileSet.keySet()){
                    iDF = Math.log10(files.size() / (double) i);
                    tFiDF = Double.parseDouble(fileSet.get(key))*iDF;
                    System.out.println(word+"--"+key + "--" +fileSet.get(key)+"--"+iDF+"--"+tFiDF);
                    wordFiles = new Text(word+"####"+key);
                    context.write(wordFiles,new DoubleWritable(tFiDF));
                }
            }
        }
    }



