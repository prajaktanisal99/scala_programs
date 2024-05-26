// import java.io.*;
// import java.util.*;
// import org.apache.hadoop.conf.*;
// import org.apache.hadoop.fs.Path;
// import org.apache.hadoop.io.*;
// import org.apache.hadoop.mapreduce.*;
// import org.apache.hadoop.mapreduce.lib.input.*;
// import org.apache.hadoop.mapreduce.lib.output.*;

// class Block implements Writable {
//     int rows;
//     int columns;
//     public double[][] data;

//     Block () {}

//     Block (int rows, int columns) {
//         this.rows = rows;
//         this.columns = columns;
//         data = new double[rows][columns];
        
//     }

//     Block (int rows, int columns, double[][] data ) {
//         this.rows = rows;
//         this.columns = columns;
//         this.data = new double[rows][columns];
//         for (int i = 0; i < this.rows; i++) {
//             for (int j = 0; j < this.columns; j++) {
//                 this.data[i][j] = data[i][j];
//             }
//         }
//     }

//     @Override
//     public String toString () {
//         String s = "\n";
//         for ( int i = 0; i < rows; i++ ) {
//             for ( int j = 0; j < columns; j++ )
//                 s += String.format("\t%.3f",data[i][j]);
//             s += "\n";
//         }
//         return s;
//     }

//     @Override
//     public void write(DataOutput out) throws IOException {
//         out.writeInt(this.rows);
//         out.writeInt(this.columns);
//         for (int i = 0; i < rows; i++) {
//             for (int j = 0; j < columns; j++) {
//                 out.writeDouble(this.data[i][j]);
//             }
//         }
//     }

//     @Override
//     public void readFields(DataInput in) throws IOException {
//         this.rows = in.readInt();
//         this.columns = in.readInt();
//         this.data = new double[this.rows][this.columns];
//         for (int i = 0; i < rows; i++) {
//             for (int j = 0; j < columns; j++) {
//                 data[i][j] = in.readDouble();
//             }
//         }
//     }
// }

// class Pair implements WritableComparable<Pair> {
//     private int i;
//     private int j;
	
//     Pair () {}
	
//     Pair ( int i, int j ) { this.i = i; this.j = j; }

//     @Override
//     public String toString () {
//         return ""+i+"\t"+j;
//     }

//     @Override
//     public void write(DataOutput out) throws IOException {
//         out.writeInt(i);
//         out.writeInt(j);
//     }

//     @Override
//     public void readFields(DataInput in) throws IOException {
//         i = in.readInt();
//         j = in.readInt();
//     }

//     @Override
//     public int compareTo(Pair o) {
//         if (i == o.i) {
//             return Integer.compare(j, o.j);
//         }
//         return Integer.compare(i, o.i);
//     }
// }

// class Triple implements Writable {
//     int i;
//     int j;
//     double value;
	
//     Triple () {}
	
//     Triple ( int i, int j, double v ) { this.i = i; this.j = j; value = v; }

//     @Override
//     public String toString () {
//         return ""+i+"\t"+j+"\t"+value;
//     }

//     @Override
//     public void write(DataOutput out) throws IOException {
//         out.writeInt(i);
//         out.writeInt(j);
//         out.writeDouble(value);
//     }

//     @Override
//     public void readFields(DataInput in) throws IOException {
//         i = in.readInt();
//         j = in.readInt();
//         value = in.readDouble();
//     }

// }

// public class AddSample {

//     public static class SparseToBlockMapper extends Mapper<Object, Text, Pair, Triple> {

//         @Override
//         protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
//             Configuration configuration = context.getConfiguration();
//             int r = configuration.getInt("rows", 0);
//             int c = configuration.getInt("columns", 0);
            
//             try (
//                 Scanner scanner = new Scanner(value.toString());
//             ) {
                
//                 String[] pairValues = scanner.nextLine().split(",");
//                 int i = Integer.parseInt(pairValues[0]);
//                 int j = Integer.parseInt(pairValues[1]);
//                 double v = Double.parseDouble(pairValues[2]);
//                 Pair pair = new Pair( i / r, j / c);
//                 Triple triple = new Triple( i % r, j % c, v);
                
//                 // to emit: emit( Pair(i/rows,j/columns), Triple(i%rows,j%columns,v) )
//                 context.write(pair, triple);
//             } catch (IOException e) {
//                 throw new IOException();
//             }
//         }   
//     }

//     public static class SparseToBlockReducer extends Reducer<Pair, Triple, Pair, Block> {
        
//         @Override
//         protected void reduce(Pair key, Iterable<Triple> values, Context context) throws IOException, InterruptedException {
//             Configuration configuration = context.getConfiguration();
//             int rows = configuration.getInt("rows", 0);
//             int columns = configuration.getInt("columns", 0);
//             Block block = new Block(rows, columns);
//             for (Triple triple : values) {
//                 block.data[triple.i][triple.j] = triple.value;
//             }
//             context.write(key, block);
//         }
//     }

//     public static class AddMapper extends Mapper<Pair, Block, Pair, Block> {

//         @Override
//         protected void map(Pair key, Block value, Context context) throws IOException, InterruptedException {
//             context.write(key, value);
//         }   
//     }

//     public static class AddReducer extends Reducer<Pair, Block, Pair, Block> {
        
//         @Override
//         protected void reduce(Pair key, Iterable<Block> values, Context context) throws IOException, InterruptedException {
//             int rows = context.getConfiguration().getInt("rows", 0);
//             int columns =  context.getConfiguration().getInt("columns", 0);
//             Block sumBlock = new Block(rows, columns);
//             for (Block block: values) {
//                 for (int i = 0; i < rows; i++) {
//                     for (int j = 0; j < columns; j++) {
//                         sumBlock.data[i][j] += block.data[i][j];
//                     }
//                 }
//             }
//             // to emit: key and summed block
//             context.write(key, sumBlock);
//         }
//     }

//     public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//         Configuration configuration = new Configuration();
//         configuration.setInt("rows", Integer.parseInt(args[0]));
//         configuration.setInt("columns", Integer.parseInt(args[1]));
        
//         // Job: for first file
//         Job sparseToBlock1 = Job.getInstance(configuration, "SparseToBlock");
//         sparseToBlock1.setJarByClass(AddSample.class);
//         sparseToBlock1.setOutputKeyClass(Pair.class);
//         sparseToBlock1.setOutputValueClass(Block.class);
//         sparseToBlock1.setMapperClass(SparseToBlockMapper.class);
//         sparseToBlock1.setMapOutputKeyClass(Pair.class);
//         sparseToBlock1.setMapOutputValueClass(Triple.class);
//         sparseToBlock1.setReducerClass(SparseToBlockReducer.class);
//         sparseToBlock1.setInputFormatClass(TextInputFormat.class);
//         sparseToBlock1.setOutputFormatClass(SequenceFileOutputFormat.class);
//         FileInputFormat.setInputPaths(sparseToBlock1, new Path(args[2]));
//         FileOutputFormat.setOutputPath(sparseToBlock1, new Path(args[4]));
//         sparseToBlock1.waitForCompletion(true);

//         // Job: for second file
//         Job sparseToBlock2 = Job.getInstance(configuration, "SparseToBlock");
//         sparseToBlock2.setJarByClass(AddSample.class);
//         sparseToBlock2.setOutputKeyClass(Pair.class);
//         sparseToBlock2.setOutputValueClass(Block.class);
//         sparseToBlock2.setMapperClass(SparseToBlockMapper.class);
//         sparseToBlock2.setMapOutputKeyClass(Pair.class);
//         sparseToBlock2.setMapOutputValueClass(Triple.class);
//         sparseToBlock2.setReducerClass(SparseToBlockReducer.class);
//         sparseToBlock2.setInputFormatClass(TextInputFormat.class);
//         sparseToBlock2.setOutputFormatClass(SequenceFileOutputFormat.class);
//         FileInputFormat.setInputPaths(sparseToBlock2, new Path(args[3]));
//         FileOutputFormat.setOutputPath(sparseToBlock2, new Path(args[5]));
//         sparseToBlock2.waitForCompletion(false);

//         // Job: Convert Add blocks
//         Job addBlocks = Job.getInstance(configuration);
//         addBlocks.setJobName("AddBlocks");
//         addBlocks.setJarByClass(AddSample.class);
//         addBlocks.setOutputKeyClass(Pair.class);
//         addBlocks.setOutputValueClass(Block.class);
//         addBlocks.setMapperClass(AddMapper.class);
//         addBlocks.setMapOutputKeyClass(Pair.class);
//         addBlocks.setMapOutputValueClass(Block.class);
//         addBlocks.setReducerClass(AddReducer.class);
//         addBlocks.setOutputFormatClass(TextOutputFormat.class);
//         MultipleInputs.addInputPath(addBlocks, new Path(args[4]), SequenceFileInputFormat.class, AddMapper.class );
//         MultipleInputs.addInputPath(addBlocks, new Path(args[5]), SequenceFileInputFormat.class, AddMapper.class );
//         FileOutputFormat.setOutputPath(addBlocks, new Path(args[6]));
//         addBlocks.waitForCompletion(false);

//     }
// }
