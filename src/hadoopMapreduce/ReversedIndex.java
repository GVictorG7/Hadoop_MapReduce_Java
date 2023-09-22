package hadoopMapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class ReversedIndex {
    private final Random rand = new Random();

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        List<String> stopWords = null;

        public void setup(Context context) throws IOException, InterruptedException {
            stopWords = new ArrayList<>();

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                try {
                    String line;
                    FileSystem fs = FileSystem.get(context.getConfiguration());
                    Path getFilePath = new Path(cacheFiles[0].toString());
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(getFilePath)));

                    while ((line = reader.readLine()) != null) {
                        String[] words = line.split(" ");

                        Collections.addAll(stopWords, words);
                    }
                } catch (Exception e) {
                    System.out.println("Unable to read the File");
                    System.exit(1);
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\"',.()?![]#$*-;:_+/\\<>@%& ");
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                for (String stopWord : stopWords) {
                    if (StringUtils.equalsIgnoreCase(stopWord, word)) {
                        continue;
                    }
                }
                if (!stopWords.contains(word)) {
                    String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

                    // here should be the computation of line number
                    int pseudoLine = rand.nextInt(100);

                    String filenameLine = fileName + "__" + pseudoLine; // fileName__lineNumber

                    context.write(new Text(word), new Text(filenameLine));
                }
            }
        }
    }

    public static class TextReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, List<Integer>> map = new HashMap<>();

            for (Text filenameLineNumber : values) {
                String filenameLineNumberString = filenameLineNumber.toString();
                String[] split = filenameLineNumberString.split("__");
                String fileName = split[0];
                int lineNumber = 0;
                try {
                    lineNumber = Integer.parseInt(split[1]);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("Not possible to split: " + filenameLineNumberString);
                }
                if (map.containsKey(fileName)) {
                    map.get(fileName).add(lineNumber);
                } else {
                    map.put(fileName, new ArrayList<>());
                    map.get(fileName).add(lineNumber);
                }
            }

            for (List<Integer> list : map.values()) {
                Collections.sort(list);
            }

            StringBuilder response = new StringBuilder();
            for (Map.Entry<String, List<Integer>> entry : map.entrySet()) {
                response.append(entry.getKey());
                response.append(" (");
                for (Integer lineNo : entry.getValue()) {
                    response.append(lineNo);
                    response.append(" ");
                }
                response.append(") ");
            }

            context.write(key, new Text(response.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "reversed index");
        job.setJarByClass(ReversedIndex.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(TextReducer.class);
        job.setReducerClass(TextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        try {
            job.addCacheFile(new URI("/stopWords.txt"));
        } catch (Exception e) {
            System.out.println("File Not Added");
            System.exit(1);
        }
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
