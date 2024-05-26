import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

class Vertex implements Writable {
    public short tag; // 0 for a graph vertex, 1 for a group number
    public long group; // the group where this vertex belongs to
    public long VID; // the vertex ID
    public ArrayList<Long> adjacent; // adjacent vertices

    Vertex() {
    }

    Vertex(short tag, long group, long VID, ArrayList<Long> adjacent) {
        this.tag = tag;
        this.group = group;
        this.VID = VID;
        this.adjacent = adjacent;
    }

    Vertex(short tag, long group) {
        this.tag = tag;
        this.group = group;
        this.VID = 0;
        this.adjacent = new ArrayList<>();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeShort(this.tag);
        out.writeLong(this.group);
        out.writeLong(this.VID);
        int numberOfAdjacentVertices = adjacent.size();
        out.writeInt(numberOfAdjacentVertices);
        for (int i = 0; i < numberOfAdjacentVertices; i++) {
            out.writeLong(this.adjacent.get(i));
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.tag = in.readShort();
        this.group = in.readLong();
        this.VID = in.readLong();
        int numberOfAdjacentVertices = in.readInt();
        this.adjacent = new ArrayList<>(numberOfAdjacentVertices);
        for (int i = 0; i < numberOfAdjacentVertices; i++) {
            this.adjacent.add(in.readLong());
        }
    }

    @Override
    public String toString() {
        String adjacentVertices = "\n";
        for (int i = 0; i < adjacent.size(); i++) {
            adjacentVertices += Long.toString(adjacent.get(i));
        }
        return "" + this.tag + "\t" + this.group + " \t" + this.VID + "\t" + adjacentVertices;
    }
}

public class Graph {

    public static class ReadMapper extends Mapper<Object, Text, LongWritable, Vertex> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] vertices = value.toString().split(",");
            long vertex = Long.parseLong(vertices[0]);
            ArrayList<Long> adjacent = new ArrayList<>();
            for (int i = 1; i < vertices.length; i++) {
                adjacent.add(Long.parseLong(vertices[i]));
            }
            context.write(new LongWritable(vertex), new Vertex((short) 0, vertex, vertex, adjacent));
        }
    }

    public static class PropogateMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {

        @Override
        public void map(LongWritable key, Vertex vertex, Context context) throws IOException, InterruptedException {

            LongWritable newKey = new LongWritable(vertex.VID);
            context.write(newKey, vertex);
            for (long adjacentVertex : vertex.adjacent) {
                context.write(new LongWritable(adjacentVertex), new Vertex((short) 1, vertex.group));
            }
        }
    }

    public static class PropogateReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

        @Override
        public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
                throws IOException, InterruptedException {

            long minimumValue = Long.MAX_VALUE;
            ArrayList<Long> adj = new ArrayList<>();
            for (Vertex vertex : values) {
                if (vertex.tag == 0) {
                    adj = vertex.adjacent;
                }
                minimumValue = Math.min(minimumValue, vertex.group);
            }
            context.write(new LongWritable(minimumValue), new Vertex((short) 0, minimumValue, key.get(), adj));
        }
    }

    public static class FinalMapper extends Mapper<LongWritable, Vertex, LongWritable, IntWritable> {

        @Override
        public void map(LongWritable key, Vertex values, Context context) throws IOException, InterruptedException {
            context.write(key, new IntWritable(1));
        }
    }

    public static class FinalReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

        @Override
        public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable count : values) {
                sum +=  count.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Job1");
        job.setJobName("Graph");
        job.setJarByClass(Graph.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Vertex.class);

        job.setMapperClass(ReadMapper.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Vertex.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/f0"));

        job.waitForCompletion(true);

        for (short i = 0; i < 5; i++) {
            Configuration conf2 = new Configuration();
            Job job2 = Job.getInstance(conf2, "Job2");
            job2.setJobName("Graph2");
            job2.setJarByClass(Graph.class);

            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);

            job2.setMapperClass(PropogateMapper.class);
            job2.setReducerClass(PropogateReducer.class);

            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);

            job2.setInputFormatClass(SequenceFileInputFormat.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            FileInputFormat.setInputPaths(job2, new Path(args[1] + "/f" + i));
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/f" + (i + 1)));

            job2.waitForCompletion(true);
        }

        // ... Final Map-Reduce job to calculate the connected component sizes
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Job3");
        job3.setJobName("Graph2");
        job3.setJarByClass(Graph.class);

        job3.setMapperClass(FinalMapper.class);
        job3.setReducerClass(FinalReducer.class);

        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);

        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(IntWritable.class);

        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(args[1] + "/f5"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));

        job3.waitForCompletion(true);
    }
}