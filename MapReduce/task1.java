package t1;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.map.*;

public class task1 {

    public static class GoodsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split(",");
            Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
            if (items.length == 7 && !items[6].equals("0") && pattern.matcher(items[6]).matches()) {
                word.set(items[1]);
                context.write(word, one);
            }
        }
    }

    public static class GoodsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        Map<String, Integer> map = new HashMap<String, Integer>();
        //private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //result.set(sum);
            //context.write(key, result);
            map.put(key.toString(), sum);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                //降序排序
                public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
                    return (int) (arg1.getValue() - arg0.getValue());
                }
            });
            for (int i = 0; i < 100; i++) {
                context.write(new Text(list.get(i).getKey()), new IntWritable(list.get(i).getValue()));
            }
        }
    }

    public static class MerchantsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Set<String> usrIds = new TreeSet<String>();
        private String info;
        private Configuration conf;
        private BufferedReader fis;
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        //筛选得到30岁以下年轻人
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            info = "data/user_info_format1.csv";
            FileSystem fs = FileSystem.get(URI.create(info), conf);
            FSDataInputStream hdfsInStream = fs.open(new Path(info));

            InputStreamReader isr = new InputStreamReader(hdfsInStream, "utf-8");
            String line;
            BufferedReader br = new BufferedReader(isr);
            br.readLine();
            while ((line = br.readLine()) != null) {
                String items[] = line.split(",");
                if (items.length < 2) {
                    continue;
                }

                String age = items[1], id = items[0];
                if (age.equals("1") || age.equals("2") || age.equals("3") || age.equals("4")) {
                    usrIds.add(id);
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split(",");
            if (items.length == 7 && usrIds.contains(items[1])) {
                word.set(items[3]);
                context.write(word, one);
            }
        }
    }

    public static class MerchantsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        Map<String, Integer> map = new HashMap<String, Integer>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            map.put(key.toString(), sum);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String, Integer>> list = new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                //降序排序
                public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
                    return (int) (arg1.getValue() - arg0.getValue());
                }
            });
            for (int i = 0; i < 100; i++) {
                context.write(new Text(list.get(i).getKey()), new IntWritable(list.get(i).getValue()));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();

        Path info = new Path("data/user_info_format1.csv");
        Path log = new Path("data/user_log_format1.csv");

        Path out1 = new Path(otherArgs[0]);
        FileSystem fileSystem = FileSystem.get(conf1);
        if (fileSystem.exists(out1)) {
            fileSystem.delete(out1, true);
            System.out.println("Job1 output exists,but it has deleted");
        }

        //job1,统计最热门100件商品
        Job job1 = Job.getInstance(conf1, "Screen the hottest goods");
        job1.setJarByClass(task1.class);

        job1.setMapperClass(GoodsMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);

        job1.setReducerClass(GoodsReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        ControlledJob ctrljob1 = new ControlledJob(conf1);
        ctrljob1.setJob(job1);
        FileInputFormat.addInputPath(job1, log);
        FileOutputFormat.setOutputPath(job1, out1);

        //job2,统计最受年轻人关注的商家
        Configuration conf2 = new Configuration();
        Path out2 = new Path(otherArgs[1]);
        if (fileSystem.exists(out2)) {
            fileSystem.delete(out2, true);
            System.out.println("Job2 output exists,but it has deleted");
        }

        Job job2 = Job.getInstance(conf2, "Most popular merchants");
        job2.setJarByClass(task1.class);

        job2.setMapperClass(MerchantsMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);

        job2.setReducerClass(MerchantsReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        ControlledJob ctrljob2 = new ControlledJob(conf2);
        ctrljob2.setJob(job2);
        FileInputFormat.addInputPath(job2, log);
        FileOutputFormat.setOutputPath(job2, out2);

        //设置job依赖关系
        ctrljob2.addDependingJob(ctrljob1);
        JobControl jobCtrl= new JobControl("my ctrl");
        jobCtrl.addJob(ctrljob1);
        jobCtrl.addJob(ctrljob2);

        Thread t = new Thread(jobCtrl);
        t.start();

        while (true) {

            if (jobCtrl.allFinished()) {// 如果作业成功完成，就打印成功作业的信息
                System.out.println(jobCtrl.getSuccessfulJobList());
                jobCtrl.stop();
                break;
            }
        }

        //System.exit(job1.waitForCompletion(true) ? 0 : 1);

        /*
        String InfoPath = "data/user_info_format1.csv";
        String LogPath = "data/user_log_format1.csv";
        BufferedReader readInfo = new BufferedReader(new FileReader(InfoPath));
        BufferedReader readLog = new BufferedReader(new FileReader(LogPath));
        readInfo.readLine();
        String line = null;
        int limit = 50000, i = 0;
        while ((line = readInfo.readLine()) != null && i < limit) {
            String item[] = line.split(",");
            if (item.length < 2) {
                continue;
            }

            String age = item[1], id = item[0];
            if (age.equals("1") || age.equals("2") || age.equals("3") || age.equals("4")) {
                System.out.println("id:" + id + "; age:" + age);
                i++;
            }
        }
        System.out.println("Count:" + i);
        */
    }
}
