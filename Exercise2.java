import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import java.lang.Math;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class Exercise2
{
	public static class TripMapper extends Mapper<Object, Text, Text, Text> 
	{
		SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    	{
   			String valueStr = value.toString();
   			int index = valueStr.indexOf(',');
   			String[] tokens = value.toString().split(",");
   			int length = tokens.length;
   			
   			if(length ==9){
   				String keyUserID = tokens[0].trim();
    			String startDateTime = tokens[1].trim();
        		String startPosLat = tokens[2].trim();
        		String startPosLong = tokens[3].trim();
        		String startStatus = tokens[4].trim();
				String endDateTime = tokens[5].trim();
				String endPosLat = tokens[6].trim();
				String endPosLong = tokens[7].trim();
				String endStatus = tokens[8].trim();
				
        		if(!keyUserID.equals("NULL")&& !keyUserID.isEmpty() && !startDateTime.equals("NULL") && !startDateTime.isEmpty() && !startPosLat.equals("NULL") && !startPosLat.isEmpty() && !startPosLong.equals("NULL") && !startPosLong.isEmpty() && !startStatus.equals("NULL") && !startStatus.isEmpty() && !endDateTime.equals("NULL") && !endDateTime.isEmpty() && !"NULL".equals(endPosLat) && !endPosLat.isEmpty() && !endPosLong.equals("NULL") && !endPosLong.isEmpty() && !endStatus.equals("NULL") && !endStatus.isEmpty()) 
        		{ 
            		context.write(new Text(keyUserID + "," + startDateTime), new Text(valueStr.substring(index + 1)));
        		}
        	}	
    	} 		
	}
	public static class KeyComparator extends WritableComparator 
	{
 		protected KeyComparator() 
 		{
 			super(Text.class, true);
 		}
		@Override
 		public int compare(WritableComparable w1, WritableComparable w2) 
 		{
 			SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Text t1 = (Text) w1;
 			Text t2 = (Text) w2;
 			String[] t1Items = t1.toString().split(",");
 			String[] t2Items = t2.toString().split(",");
 			String keyUserID1String = t1Items[0];
 			String keyUserID2String = t2Items[0];
 			int keyUserID1 = Integer.parseInt(keyUserID1String);
 			int keyUserID2 = Integer.parseInt(keyUserID2String); 
 			String startDateTime1 = t1Items[1];
 			String startDateTime2 = t2Items[1];
 			String[] startDateTimeArray1= startDateTime1.split("'");
 			String[] startDateTimeArray2= startDateTime2.split("'");
 			String startDateTimeFinal1 = startDateTimeArray1[1];
 			String startDateTimeFinal2 = startDateTimeArray2[1];
 			int comp = keyUserID1String.compareTo(keyUserID2String);
 			
 			try{
 				Date d1= format2.parse(startDateTimeFinal1);
 				Date d2= format2.parse(startDateTimeFinal2);
 				
 				if (comp == 0) {
 					comp = d2.compareTo(d1);
 				}
 			} catch(Exception e) {} 
 			
			return comp;
		}
	}
 	public static class GroupComparator extends WritableComparator 
 	{
		protected GroupComparator() 
		{
			super(Text.class, true);
		}
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) 
		{
 			Text t1 = (Text) w1;
 			Text t2 = (Text) w2;
 			String[] t1Items = t1.toString().split(",");
 			String[] t2Items = t2.toString().split(",");
 			String keyUserID1String = t1Items[0];
 			String keyUserID2String = t2Items[0];
 			int keyUserID1 = Integer.parseInt(keyUserID1String);
 			int keyUserID2 = Integer.parseInt(keyUserID2String);
 			int comp = keyUserID1String.compareTo(keyUserID2String);
 			
 			return comp;
 		}
 	}
 	public static class TripPartitioner extends Partitioner <Text, Text>
 	{
 		@Override
 		public int getPartition(Text key, Text val, int numPartitions) 
 		{
 			String[] keyItems = key.toString().split(",");
 			String keyBase = keyItems[0];
 			int part  = keyBase.hashCode() % numPartitions;
 			
 			return part;
 		}
 	}
	public static class TripReducer extends Reducer<Text, Text, Text, Text> 
	{
		
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {	
        	double avgSpeed=0,distanceTotal=0,timeDifferenceTotal=0, timeDifference=0,  speed=0,  totSpeed=0, distance=0,doubleStartPosLatPrevious=0, doubleStartPosLongPrevious=0;
    		boolean airport=false, exceed=false, status=false, takeClient=false, dropClient=false;
    		int count = 0, airportCount=0, clientNumber=0, segment=0 ,indicator =12850183, timeDifferenceMinutes=0;
    		SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    		String endPosLatInitial="Null",endPosLongInitial ="Null",endDateTimeInitial = "Null";
			Date d1, d2;
			
    		for (Text value : values)
    		{
    			try{
        		String[] tokens2 = value.toString().split(",");
       			String startDateTime = tokens2[0].trim();
        		String startPosLat = tokens2[1].trim();
    			String startPosLong = tokens2[2].trim();
    			String startStatus = tokens2[3].trim();
				String endDateTime = tokens2[4].trim();
				String endPosLat = tokens2[5].trim();
				String endPosLong = tokens2[6].trim();
				String endStatus = tokens2[7].trim();
				double latAirport=Math.toRadians(37.62131),lonAirport=Math.toRadians(-122.37896),startFee=3.5,additionFee=1.71,radius=1,pi=3.14,area;
				double earth_radius = (double) 6373.0;
				double doubleStartPosLat = Double.parseDouble(startPosLat);
				double doubleStartPosLong = Double.parseDouble(startPosLong);
				double doubleEndPosLat = Double.parseDouble(endPosLat);
				double doubleEndPosLong = Double.parseDouble(endPosLong);
				double dlat=(double)Math.toRadians(doubleEndPosLat- doubleStartPosLat), dlon=(double)Math.toRadians(doubleEndPosLong-doubleStartPosLong);
				double distanceAirport = 6371 * Math.acos(Math.sin(latAirport) * Math.sin(Math.toRadians(doubleEndPosLat)) + Math.cos(latAirport) * Math.cos(Math.toRadians(doubleEndPosLat)) * Math.cos(Math.toRadians(doubleEndPosLong) - lonAirport));
				double a = (double) (Math.pow(Math.sin(dlat/2),2) + Math.cos(Math.toRadians(doubleEndPosLat)) * Math.cos(Math.toRadians(doubleStartPosLat)) * Math.pow(Math.sin(dlon/2),2));	
   				distance = (double)((earth_radius * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))));	
   				indicator--;
   				segment++;		
				String[] startDateTimeArray2=startDateTime.split("'"), endDateTimeArray2 = endDateTime.split("'");
				String startDateTime2=startDateTimeArray2[1], endDateTime2=endDateTimeArray2[1];
				d1= format2.parse(startDateTime2);
        		d2= format2.parse(endDateTime2);
        		timeDifference = ((d2.getTime() - d1.getTime())/1000);
        		speed = ((distance/ timeDifference)*60*60);
        		
        		if(startStatus.equals("'E'") && endStatus.equals("'M'"))
        		{
        			clientNumber++;
        		}
        		
        		if(startStatus.equals("'E'") && endStatus.equals("'M'") || startStatus.equals("'M'") && endStatus.equals("'E'") )
        		{
        			status=true;
        		}
        		
        		if(timeDifference <= 60*60 && status ==false)
        		{
        			
        			if(count>0)
        			{
        					if(doubleStartPosLatPrevious==doubleEndPosLat && doubleStartPosLongPrevious== doubleEndPosLong)
							distanceTotal += distance;
        					timeDifferenceTotal += timeDifference;
        					timeDifferenceMinutes= (int) (timeDifferenceTotal / 60); 
    						avgSpeed = (double) (distanceTotal/timeDifferenceTotal)*60*60;
    						doubleStartPosLatPrevious=doubleStartPosLat;    					
							doubleStartPosLongPrevious=doubleStartPosLong;
        			}
        			
        			else 
        			{
        				distanceTotal += distance;
        				timeDifferenceTotal += timeDifference;
        				timeDifferenceMinutes= (int) (timeDifferenceTotal / 60); 
    					avgSpeed = (double) (distanceTotal/timeDifferenceTotal)*60*60;
    					count++;	
    					doubleStartPosLatPrevious=doubleStartPosLat;    							
					doubleStartPosLongPrevious=doubleStartPosLong;
					endPosLatInitial=endPosLat;
					endPosLongInitial=endPosLong;
					endDateTimeInitial=endDateTime;
        			}
        		} 
        		
        		else 
        		{
   					
   					if(clientNumber ==0 && distanceTotal>=0 && timeDifferenceMinutes>=0 && avgSpeed>=0 && avgSpeed<200||clientNumber>0 && distanceTotal>0.5 && timeDifferenceMinutes>3 && avgSpeed>5 && avgSpeed<200)
   					{
   						distanceTotal += distance;
   						timeDifferenceTotal += timeDifference;
        				timeDifferenceMinutes= (int) (timeDifferenceTotal / 60); 
    					avgSpeed = (double) (distanceTotal/timeDifferenceTotal)*60*60;
   						
   						if(startStatus.equals("'E'") && endStatus.equals("'M'"))
   						{		
							
							if(dropClient){
								airportCount++;		
        						airport=true;
							} 
							
							else if(distanceAirport<1)
							{
								airportCount++;		
								takeClient=true;
        						airport=true;
							} 
        						context.write(key, new Text(","+startPosLat+","+startPosLong+","+endDateTimeInitial+","+endPosLatInitial+","+endPosLongInitial+","+Double.toString(distanceTotal)+ "," + Double.toString(timeDifferenceMinutes) + "," + Double.toString(avgSpeed)+ "," + Integer.toString(airportCount)+ "," +endStatus + ","+ Boolean.toString(takeClient) + ","+ Boolean.toString(dropClient)));
							dropClient=false;
							takeClient=false;
							airportCount=0;
   							airport=false;				
   							distanceTotal=distance;
   							timeDifferenceTotal=timeDifference;
        				}
        				
        				if(startStatus.equals("'M'") && endStatus.equals("'E'"))
        				{
        					
        					context.write(key, new Text(","+startPosLat+","+startPosLong+","+endDateTimeInitial+","+endPosLatInitial+","+endPosLongInitial+","+Double.toString(distanceTotal)+ "," + Double.toString(timeDifferenceMinutes) + "," + Double.toString(avgSpeed)+ "," + Integer.toString(airportCount)+ "," +endStatus + ","+ Boolean.toString(takeClient) + ","+ Boolean.toString(dropClient)));	
        					
        					if(distanceAirport<1)
        					{
        						dropClient=true;
        						airport=true;
        					}
        					
        					distanceTotal=distance;
   							timeDifferenceTotal=timeDifference;
       
        				}
						distanceTotal=0;
   						timeDifferenceTotal=0;
   						timeDifferenceMinutes=0;
   						count=0;
   						clientNumber=0;
   						status=false;
   						segment=0;
   					} 
   					else {
   						distanceTotal=0;
   						timeDifferenceTotal=0;
   						timeDifferenceMinutes=0;
   						count=0;
   						airportCount=0;
   						airport=false;
   						clientNumber=0;
   						exceed = false;
   						status=false;
   						takeClient=false;
   						dropClient=false;
   						segment=0;
   					}
   				}
        	} 
        	catch (ParseException e) 
        		{ e.printStackTrace(); }		
			}	
		}             	   
	}
	
	public static class RevenueMapper extends Mapper<Object, Text, Text, DoubleWritable> 
	{
		SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date d1, d2;
		double revenue=0;
		private final static DoubleWritable result = new DoubleWritable();
    	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    	{
   			String valueStr = value.toString();
   			int index = valueStr.indexOf(',');
   			String[] tokens = value.toString().split(","); 	
   			String keyUserID = tokens[0].trim();
    			String startDateTime = tokens[1].trim();
        		String distance = tokens[7].trim();
        		String duration = tokens[8].trim();
        		String speed = tokens[9].trim();
			String airport = tokens[10].trim();
			double doubleDistance = Double.parseDouble(distance);
			
			String[] startDateTimeArray2=startDateTime.split("'");
			String startDateTime2=startDateTimeArray2[1];
			String[] startDateTime3=startDateTime2.split(" ");
			String startDateTime4= startDateTime3[0];
			String[] startDateTime5=startDateTime4.split("-"); 
			String startDateTime6= startDateTime5[0];
    
        	if(!startDateTime.equals("NULL") && !distance.equals("NULL")) 
        	{
        		if(airport.equals("1"))
        		{
        			revenue = doubleDistance*1.71+3.5;
        			result.set(revenue);
        			context.write(new Text(startDateTime6), result);
        		}
        	}			
    	} 		
	}
	public static class RevenueReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> 
	{
		private DoubleWritable result = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {	
        	double totalRevenue=0;
			
			for (DoubleWritable val : values) 
			{
                totalRevenue += val.get();
            }
			
			result.set(totalRevenue);
			context.write(key, result);
		}             	   
	}
    public int run(String args[]) throws Exception 
    {
    	JobControl jobControl = new JobControl("jobChain");
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "JOB_1");
        job1.setJarByClass(Exercise2.class);
        job1.setMapperClass(TripMapper.class);
        job1.setReducerClass(TripReducer.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.setJobName("Reconstructing trips");
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setPartitionerClass(TripPartitioner.class);
        job1.setSortComparatorClass(KeyComparator.class);
        job1.setGroupingComparatorClass(GroupComparator.class); 
        job1.setInputFormatClass(TextInputFormat.class);
	job1.setNumReduceTasks(105);
	FileInputFormat.setMaxInputSplitSize(job1,81000000);
        boolean success = job1.waitForCompletion(true);
        
        if (success) 
        {
      		Configuration conf2 = new Configuration();
      		Job job2 = Job.getInstance(conf2);
      		job2.setJarByClass(Exercise2.class);
      		FileInputFormat.setInputPaths(job2, new Path(args[1]));
       		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
      		job2.setMapperClass(RevenueMapper.class);
       	 	job2.setReducerClass(RevenueReducer.class);
		job2.setJobName("Compute revenue");
		job2.setOutputKeyClass(Text.class);
        	job2.setOutputValueClass(DoubleWritable.class);
        	job2.setInputFormatClass(TextInputFormat.class);
			success = job2.waitForCompletion(true);
		}
		
   		return success ? 0 : 1;   
 	} 
	public static void main(String args[]) throws Exception 
	{
    	Exercise2 j = new Exercise2();
    	int ret = j.run(args);
    	System.exit(ret);
  	}
}
	
