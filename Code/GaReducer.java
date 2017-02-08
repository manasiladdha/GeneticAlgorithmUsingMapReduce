package GA;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URI;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GaReducer extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> arrayValues, Context context) throws IOException, InterruptedException {

		int target = Integer.parseInt(context.getConfiguration().get("targetNumber"));
		int popSize = Integer.parseInt(context.getConfiguration().get("popSize"));
		int numReduceTasks = Integer.parseInt(context.getConfiguration().get("tasks"));

		ArrayList<Chromosome> pool = new ArrayList<Chromosome>(popSize/numReduceTasks);
		ArrayList<String> chromoNumbers = new ArrayList<String>(popSize/numReduceTasks);
		
		for(Text chromosomeString : arrayValues){
			String[] values = chromosomeString.toString().trim().split(",");
			String chromo = values[1].trim();
			double fitnessScore = Double.parseDouble(values[2].trim());
			Chromosome c = new Chromosome(chromo,fitnessScore);
			pool.add(c);
			chromoNumbers.add(values[0].trim());
		}
		
		// Loop until the pool has been processed
		for(int x=pool.size()-1;x>=0;x-=2) {
			// Select two members
			Chromosome n1 = selectMember(pool);
			Chromosome n2 = selectMember(pool);

			// Cross over and mutate
			n1.crossOver(n2);
			n1.mutate();
			n2.mutate();

			// Re-score the nodes
			n1.scoreChromo(target);
			n2.scoreChromo(target);

			// Check to see if either is the solution
			if (n1.getTotal() == target && n1.isValid()) { 
				writeSolution(n1);
			}
			if (n2.getTotal() == target && n2.isValid()) { 
				writeSolution(n2); 
			}

			// Add to the new pool
			context.write(new Text(), new Text(chromoNumbers.remove(chromoNumbers.size()-1).trim() +","+n1.getChromo()+","+n1.getFitnessScore()));
			context.write(new Text(), new Text(chromoNumbers.remove(chromoNumbers.size()-1).trim() +","+n2.getChromo()+","+n2.getFitnessScore()));
		}


	}

	private Chromosome selectMember(ArrayList<Chromosome> l) { 

		// Get the total fitness		
		double tot=0.0;
		for (int x=l.size()-1;x>=0;x--) {
			double score = ((Chromosome)l.get(x)).getFitnessScore();
			tot+=score;
		}
		Random rand = new Random();
		double slice = tot*rand.nextDouble();

		// Loop to find the node
		double ttot=0.0;
		for (int x=l.size()-1;x>=0;x--) {
			Chromosome node = (Chromosome)l.get(x);
			ttot+=node.getFitnessScore();
			if (ttot>=slice) { 
				l.remove(x);
				return node; 
			}
		}

		return (Chromosome)l.remove(l.size()-1);
	}

	public static void writeSolution(Chromosome solutionChromosome){
		try{
			FileSystem fs = FileSystem.get(new URI("hdfs://localhost:9000"), new Configuration());
			Path popFile = new Path(fs.getWorkingDirectory() + "/ga/solutionChromo.txt");
			if ( fs.exists( popFile )) { fs.delete( popFile, true ); } 
			BufferedWriter writer=new BufferedWriter(new OutputStreamWriter(fs.create(popFile,true)));

			writer.write(solutionChromosome.getChromo() + "|" + 
					Double.toString(solutionChromosome.getFitnessScore())+ 
					"|DecodedChromo" + solutionChromosome.decodeChromo() +"\n");

			writer.close();
			fs.close();
		}
		catch(Exception e){
			System.err.println("Exception : writeSolution" +  e.getMessage());
		}
	}
}

