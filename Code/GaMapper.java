package GA;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GaMapper extends Mapper<LongWritable, Text, Text, Text> {

	protected void map(LongWritable key, Text chromosomeString, Context context) throws IOException, InterruptedException {

		int popSize = Integer.parseInt(context.getConfiguration().get("popSize"));
		int numReduceTasks = Integer.parseInt(context.getConfiguration().get("tasks"));
		int outkey = 0;
		
		if (numReduceTasks > 1){
			ArrayList<String> slots = generateIslandSlots(numReduceTasks, popSize);
			int chromoNumber = Integer.parseInt(chromosomeString.toString().trim().split(",")[0].trim());
			outkey = findTheIslandReducer(chromoNumber, slots);			
		}
	
		context.write(new Text(Integer.toString(outkey)), chromosomeString);
		
	}
	
	public ArrayList<String> generateIslandSlots(int numOfReducers, int popSize){
		int numberOfSlots = numOfReducers;
		int slotSize = popSize/numberOfSlots;
		ArrayList<String> slotList = new ArrayList<>();
		int lastLowerBound = -1;
		while(numberOfSlots > 0){
			slotList.add(Integer.toString(lastLowerBound + 1) + "-" + Integer.toString(lastLowerBound+slotSize));
			lastLowerBound = lastLowerBound + slotSize;
			numberOfSlots--;
		}
		return slotList;
	}
	
	public int findTheIslandReducer(int chromoNumber, ArrayList<String> slots){
		
		for(int i = 0 ; i < slots.size() ; i++){
			String slot = slots.get(i);
			int lowerBound = Integer.parseInt(slot.trim().split("-")[0]);
			int upperBound = Integer.parseInt(slot.trim().split("-")[1]);
			if(chromoNumber >= lowerBound && chromoNumber <=upperBound){
				return i;
			}
		}
		
		return 0;
	}

}
