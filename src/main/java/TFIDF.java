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
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Pattern;


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

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), " wordcount ");
        job.setJarByClass(this.getClass());
        Job secondJob = Job.getInstance(getConf(), " tfidf ");
        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("Finishing First");
        if (job.waitForCompletion(true)) {

            secondJob.setJarByClass(this.getClass());
            FileInputFormat.addInputPaths(secondJob, args[1]);
            FileOutputFormat.setOutputPath(secondJob, new Path(args[1] + "/output1"));
            secondJob.setMapperClass(Map2.class);
            secondJob.setReducerClass(Reduce2.class);
            secondJob.setOutputKeyClass(Text.class);
            secondJob.setOutputValueClass(Text.class);
        }

        return secondJob.waitForCompletion(true) ? 0 : 1;


    }


    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

        public void map(LongWritable offset, Text lineText, Context context)
                throws IOException, InterruptedException {

            String line = lineText.toString();
            Text currentWord = new Text();
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName().toString();
            for (String word : WORD_BOUNDARY.split(line)) {
                if (word.isEmpty()) {
                    continue;
                }

                currentWord = new Text(word + "####" + fileName);
                context.write(currentWord, one);
            }
        }
    }

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
//                    System.out.println(logValues);
                }
            }
            context.write(word, new DoubleWritable(logValues));
        }
    }

    //////


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
//            System.out.println(key + "   ," + value1 + "=" + value2);
//            System.out.println("#############################");
            currentWord = new Text(key);
            valuePair = new Text(value1 + "=" + value2);

            context.write(currentWord, valuePair);
        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, DoubleWritable> {
        @Override
        public void reduce(Text word, Iterable<Text> texts, Context context)
                throws IOException, InterruptedException {

            System.out.println("Red2");
            System.out.println(word);

//            int sum  = 0;
//            for ( IntWritable count  : counts) {
//                sum  += count.get();
//            }

            for (Text text : texts) {
//                System.out.println(text.toString());
                valuepairs = text.toString().split("=");
                files.add(valuepairs[0]);
                System.out.println(valuepairs[0] + " and " + valuepairs[1]+" HashSet here "+files.size());
                }
            }
//            context.write(word, new DoubleWritable(logValues));
        }
    }



