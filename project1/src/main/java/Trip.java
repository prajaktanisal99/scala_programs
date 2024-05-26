import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.Scanner;

public class Trip {

    /* put your Map-Reduce methods here */
    public static class TripMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException, RuntimeException {

            try (
                Scanner scanner = new Scanner(value.toString());
            ) {
                String[] details = scanner.nextLine().split(",");
                if (!Character.isAlphabetic(details[0].charAt(0))) {
                    int tripDistance = (int) Math.round(Double.parseDouble(details[4]));
                    double tripFare = Double.parseDouble(details[details.length - 1]);
                    if (tripDistance >= 0 && tripDistance < 200) {
                        context.write(new IntWritable(tripDistance), new DoubleWritable(tripFare));
                    }
                }
            } catch (IOException e) {
                throw new IOException(e);
            }
        }
    }

    public static class TripReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        DecimalFormat format = new DecimalFormat("###.######");

        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context) throws  IOException, InterruptedException {

            double fareSum = 0.0;
            int distanceCount = 0;
            for (DoubleWritable fare: values) {
                fareSum += fare.get();
                distanceCount++;
            }
            double average;
            if (distanceCount == 0) {
                average = fareSum;
            } else {
                average = fareSum/distanceCount;
            }
            format.setRoundingMode(RoundingMode.FLOOR);
            String value = format.format(average);
            context.write(key, new DoubleWritable(Double.parseDouble(value)));
        }
    }

    public static void main ( String[] args ) throws Exception {

        /* put your main program here */
        Job job = Job.getInstance();
        job.setJobName("TripJob");
        job.setJarByClass(Trip.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setMapperClass(TripMapper.class);
        job.setReducerClass(TripReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(false);
    }
}
