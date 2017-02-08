package GA;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class GaDriver {
	public static void main(String[] args) {

		try {

			int gen =  0;

			Configuration conf = new Configuration();
			GenericOptionsParser parser = new GenericOptionsParser(conf, args);
			args = parser.getRemainingArgs();
			int target = Integer.parseInt(args[1]);
			conf.set("targetNumber" , args[0]); // targetNumber
			int popSize = Integer.parseInt(args[1]); // Must be even, coz we select two chromosomes from this
			conf.set("popSize" , args[1]); 
			int tasksNum = Integer.parseInt(args[2]); // Number of tasks
			conf.set("tasks" , args[2]);
			
			if(!validNumberOfTasks(popSize, tasksNum)){
				System.err.println("Number of Reducer Tasks will not evenly divide the population : " + (popSize/tasksNum));
				return;
			}
			generatePopulation(popSize, conf); // un-comment when running in psuedo/distributed mode, else comment  
//			generatePopulation(popSize, target); // un-comment in standalone mode, else comment

			while(!solutionFound(conf)){ // un-comment when running in psuedo/distributed mode, else comment
//			while(!solutionFound()){ // un-comment in standalone mode, else comment

				Job job = new Job(conf, "genetic_algorithm_for_target_generation-"+gen);
				job.setJarByClass(GaDriver.class);
				// specify output types
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);

				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);

				job.setNumReduceTasks(tasksNum);

				FileInputFormat.setInputPaths(job, new Path("ga/input-"+gen));
				gen++;
				FileOutputFormat.setOutputPath(job, new Path("ga/input-"+gen));

				// specify a mapper
				job.setMapperClass(GaMapper.class);
				//specify a partitioner
				job.setPartitionerClass(GaPartitioner.class);
				// specify a reducer
				job.setReducerClass(GaReducer.class);

				System.out.println(job.waitForCompletion(true));

			}


		}
		catch(Exception e){
			System.err.println(e.getMessage());
		}

		System.out.println("Done");
	}		


	private static boolean validNumberOfTasks(int popSize, int tasksNum) {
		if(popSize%tasksNum == 0)
			if((popSize/tasksNum)%2==0)
				return true;
		return false;
	}


	public static boolean generatePopulation(int poolSize, Configuration conf){
		int target = Integer.parseInt(conf.get("targetNumber"));

		ArrayList<Chromosome> pool = intializePool(poolSize, target);
		try{
			FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
			Path popFile = new Path(fs.getWorkingDirectory()+"/ga/input-0/population.txt");
			if ( fs.exists( popFile )) { fs.delete( popFile, true ); } 
			BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(fs.create(popFile,true)));
			
			for(Chromosome c: pool) {
				writer.write(--poolSize + "," +c.getChromo() + "," + Double.toString(c.getFitnessScore())+"\n");
			}
			writer.close();
			fs.close();
			return true;
		}
		catch(Exception e){
			System.err.println("Exception : "+ e.getMessage());
			return false;
		}

	}

	public static boolean generatePopulation(int poolSize, int target) throws IOException, URISyntaxException{

		ArrayList<Chromosome> pool = intializePool(poolSize, target);

		File popFile = new File("ga/input-0/population.txt");
		if(popFile.exists()){
			popFile.delete();
		}

		try (Writer writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(popFile)))) {
			for(Chromosome c: pool) {
				writer.write(--poolSize + "," +c.getChromo() + "," + Double.toString(c.getFitnessScore())+"\n");
			}			
			return true;
		}
		catch(Exception e){
			System.err.println("Exception : "+ e.getMessage());
			return false;
		}

	}
	
	public static ArrayList<Chromosome> intializePool(int poolSize, int target){
		ArrayList<Chromosome> pool = new ArrayList<Chromosome>(poolSize);

		// Generate unique chromosomes in the pool	
		for (int x=0;x<poolSize;x++) 
			pool.add(new Chromosome(target));

		return pool;
	}

	public static boolean solutionFound(Configuration conf) throws IOException, URISyntaxException{

		FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), conf);
		Path solutionFile = new Path(fs.getWorkingDirectory()+"/ga/solutionChromo.txt");
		if ( fs.exists( solutionFile )){
			return true;
		}
		return false;

	}

	public static boolean solutionFound() throws IOException, URISyntaxException{

		File solFile = new File("ga/solutionChromo.txt");
		if(solFile.exists()){
			return true;
		}
		return false;
	}

}
