import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CactusPlotGenerator {
	
//	primo mapper implementato che preleva il solver, il tempo, per ogni test "solved"
	public static class CactusMap extends Mapper<LongWritable, Text, MyComparable, Text>{
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, MyComparable, Text>.Context context)
				throws IOException, InterruptedException {
			String [] splitted_file = value.toString().split("\t");
			
//			System.out.println("key " +splitted_file[0] + " ---- val " +splitted_file[11]);
			if(!splitted_file[0].equals("Solver") && splitted_file[14].equals("solved"))
				context.write(new MyComparable(splitted_file[0],splitted_file[11]), new Text(splitted_file[0]));
			
		}
	}
	
//	codice non finito per la stampa in colonna
	public static class CactusReducer extends Reducer<MyComparable, Text, Text, Text>{
		
		Map<String, ArrayList<String>> matrix = new HashMap<>();
		
		@Override
		protected void reduce(MyComparable key, Iterable<Text> value, Reducer<MyComparable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			/*String output = "";
			for (Text text : value) {
				output += text+" ";
			}*/
			
			String local_key = key.id;
			if(!matrix.containsKey(local_key))
				matrix.put((local_key), new ArrayList<>());
			
			matrix.get(key).add(key.d);
//			for (Text text : value) {
//				matrix.get(local_key).add(text.toString());
//			}
//			context.write(new Text(output), new Text(key.d));
		}

		

		@Override
		protected void cleanup(Reducer<MyComparable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			System.out.println("MAP ----------------- " + matrix.keySet().size());
			String header = ""; int max = 0;
			for (String k : matrix.keySet()) {
				header += k+"\t";
				if(matrix.get(k).size() > max)
					max = matrix.get(k).size();
			}
			System.out.println("HEADER " + header + "  " + max);
			context.write(new Text(""), new Text(header));
//			int j = 0;
			for (int i = 0; i < max; i++){
				String x = "";
				for (String key : matrix.keySet()){
					if(matrix.get(key).size()>i)
						x += matrix.get(key).get(i)+ "\t";
					else
						x += "-\t";
				}
				System.out.println("---------" + x);
				context.write(new Text(""), new Text(x));
			}
		}
	}
	
//	implementazione dell'interfaccia writablecomparable per l'ordinamento sui valori
	static class MyComparable implements WritableComparable<MyComparable>{

		private String id, d; 
		
		public MyComparable() {}

		public MyComparable(String id, String d) {
			super();
			this.id = id;
			this.d = d;
		}



		@Override
		public void readFields(DataInput arg0) throws IOException {
			id = WritableUtils.readString(arg0);
			d = WritableUtils.readString(arg0);
		}


		@Override
		public String toString() {
			return "MyComparable [id=" + id + ", d=" + d + "]";
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			WritableUtils.writeString(arg0, id);
			WritableUtils.writeString(arg0, d);
		}

		@Override
		public int compareTo(MyComparable o) {
			String id1 = id, id2 = o.id;
			Double d1 = Double.parseDouble(d);
			Double d2 = Double.parseDouble(o.d);
			if(id1.equals(id2))
				return d1.compareTo(d2);
			return id1.compareTo(id2);
		}
		
	}
	public static class DoubleComparator extends WritableComparator{
		public DoubleComparator() {
			super(MyComparable.class,true);
		}
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			MyComparable a1 = (MyComparable) a;
			MyComparable b1 = (MyComparable) b;
			return super.compare(a1, b1);
		}
	}

//	secondo mapper e reducer usato per provare
	public static class WriterMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String [] splitted = value.toString().split(" ");
			String k = splitted[0],v = splitted[splitted.length-1];
			context.write(new Text(k), new Text(v));
		}
	}
	
	public static class WriterReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> iterator, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			
			/*String local_key = key.toString();
			if(!matrix.containsKey(local_key))
				matrix.put((local_key), new ArrayList<>());
			for (Text text : iterator) {
				matrix.get(local_key).add(text.toString());
			}*/
//				matches("(\\d+.\\d*)"))
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"CactusJob");
		job.setMapperClass(CactusMap.class);
		job.setReducerClass(CactusReducer.class);
		job.setSortComparatorClass(DoubleComparator.class);
		
		job.setMapOutputKeyClass(MyComparable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
	/*	boolean success = job.waitForCompletion(true);
		
		if (success) {
			Job job2 = Job.getInstance(conf,"JOB2");
			
			job2.setMapperClass(WriterMapper.class);
			job2.setReducerClass(WriterReducer.class);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			
			FileInputFormat.addInputPath(job2, new Path(args[1]));
			FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/finale"));
			
			job2.waitForCompletion(true);
		}*/
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
