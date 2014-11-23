/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Hashtable;
import java.util.Random;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileAsBinaryOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCount {

  private static final int N = 12; // NUMBER OF DOCS
  private static final int X = 6; // NUMBER OF THEME TEAMS
	
	
  public static class Hash {
	  
	  public Hashtable<String, Integer> hashTable = new Hashtable<String, Integer>();
	   
	  public Hash(Path path) {
		  // create hashtable based on stopwords.txt which is loaded from distributed cache
		  try {
			  BufferedReader reader = new BufferedReader(new FileReader(path.toString()));
			  String line = null;
			  while ((line = reader.readLine()) != null) {
				  hashTable.put(line, 1);
			  }
		  
			  reader.close();
		  } catch (IOException e) {
			  // TODO Auto-generated catch block
			  e.printStackTrace();
		  }
	  }
	  
  }
  
  // ------------------------ Phase 1 -------------------------------------
	
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Stemmer stemmer = new Stemmer();
    private String str;
    private Hash hash;
    private Path[] localFiles;
    
    
    // get distributed cache contents
    public void setup(Context context) {
		
		try {
			  localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			  
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    }
    
    
    // check for . , ' " ( ) [ ] in a given string, if found, remove them
    private String cleanString(String str) {
    	
    	String newStr = str, temp;
    	
    	// check str's first 2 characters
		if (newStr.substring(0, 1).equals(".") || newStr.substring(0, 1).equals(",") ||
				newStr.substring(0, 1).equals("'") || newStr.substring(0, 1).equals("\"")
				|| newStr.substring(0, 1).equals("(") || newStr.substring(0, 1).equals("[")) {
			newStr = newStr.substring(1, newStr.length());
		}
		if (newStr.substring(0, 1).equals(".") || newStr.substring(0, 1).equals(",") ||
				newStr.substring(0, 1).equals("'") || newStr.substring(0, 1).equals("\"")
				|| newStr.substring(0, 1).equals("(") || newStr.substring(0, 1).equals("[")) {
			newStr = newStr.substring(1, newStr.length());
		}
		
		// check str's last 2 characters
		if (newStr.substring(newStr.length() - 1).equals(".") || newStr.substring(newStr.length() - 1).equals(",") ||
				newStr.substring(newStr.length() - 1).equals("'") || newStr.substring(newStr.length() - 1).equals("\"")
				|| newStr.substring(newStr.length() - 1).equals(":") || newStr.substring(newStr.length() - 1).equals(")")
				|| newStr.substring(newStr.length() - 1).equals("]")) {
			newStr = newStr.substring(0, newStr.length() - 1);
		}
		if (newStr.substring(newStr.length() - 1).equals(".") || newStr.substring(newStr.length() - 1).equals(",") ||
				newStr.substring(newStr.length() - 1).equals("'") || newStr.substring(newStr.length() - 1).equals("\"")
				|| newStr.substring(newStr.length() - 1).equals(":") || newStr.substring(newStr.length() - 1).equals(")")
				|| newStr.substring(newStr.length() - 1).equals("]")) {
			newStr = newStr.substring(0, newStr.length() - 1);
		}

    	
    	return newStr;
    	
    }
    
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());

      setup(context); // get cache contents
      hash = new Hash(localFiles[0]); // send stopwords.txt from cache to hashtable
      
      while (itr.hasMoreTokens()) {
    	
    	  String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
    	  fileName = fileName.substring(0, fileName.length()-4);

    	  one.set(Integer.parseInt(fileName)); // doc's name/id
          word.set(itr.nextToken().toLowerCase()); // word in doc

          str = word.toString();
          str = cleanString(str);
          //Stopwords
          if (hash.hashTable.get(str) == null) { // if word not a stopword
        	
        	  // perform stemming
              stemmer.add(str.toCharArray(), str.length());
              stemmer.stem();
              word.set(stemmer.toString());
        	
              //Write word and doc id
              context.write(word, one);
          }
        
      }
    }
  }

  
  public static class IntSumReducer 
       extends Reducer<Text,IntWritable,Text,VectorWritable> {
	  
    private VectorWritable vectorWritable ;

    
    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      
      vectorWritable=  new VectorWritable(context.getConfiguration().getInt("N",0));
      
      for (IntWritable val : values) {
    	  
    	vectorWritable.set(val.get()); // create vector for current word
    	
      }
      
      // write word and vector
      context.write(key, vectorWritable);
      
    }
    
  }
  
  // --------------------------- End of Phase 1 --------------------------------
  
  // --------------------------- Phase 2 ---------------------------------------

  public static class MapperPhaseTwo 
  		extends Mapper<Text, VectorWritable, Text, VectorWritable>{
	
	private IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private VectorWritable  vec = new VectorWritable(N);
	private Path[] localFiles;
	
	
	// calculate cosine similarity between two vectors v1 and v2
	private float CosineSimilarity(VectorWritable v1, VectorWritable v2) {	
		float sum1 = 0, sum2 = 0, sum3 = 0;
		
		for (int i = 0; i < X; i++) {
			sum1 += v1.get(i)*v2.get(i);
			sum2 += v1.get(i)*v1.get(i);
			sum3 += v2.get(i)*v2.get(i);
		}
		sum2 = (float) Math.sqrt(sum2);
		sum3 = (float) Math.sqrt(sum3);
		
		return (1 - (sum1/(sum2*sum3)));
	}
	
	// get distributed cache contents
	public void setup(Context context) {
		
		try {
			  localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			  // put recently uploaded file containing team vec centers in pos 1 of cache
			  localFiles[1] = localFiles[localFiles.length-1]; 
			  
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    }
	
	
	
	public void map(Text key, VectorWritable value, Context context
	               ) throws IOException, InterruptedException {

		setup(context);
		
		String str;
		StringTokenizer itr;
		VectorWritable []centers = new VectorWritable[X];
	    int pos,i;  
	    
	    // read vector centers from cache and store them in centers array
	    BufferedReader readBuffer1 = new BufferedReader(new FileReader(localFiles[1].toString()));
		String line;
		i=0;
	    while ((line=readBuffer1.readLine())!=null){
	    	
	    	pos = 0;
	    	centers[i] = new VectorWritable(N);
	    	itr = new StringTokenizer(line);
	    	while (itr.hasMoreTokens()) {
	    		centers[i].set(pos, Float.parseFloat(itr.nextToken()));
	    		pos++;
	    	}
	    	
	    	i++;
		}
		readBuffer1.close();
	    

		// caclulate cosine similarity and assign word to a team
	    float result;
	    int minPos = 0;
	    float minVal = CosineSimilarity(value, centers[0]);
	    for (int j =1; j<X; j++){
	    	result = CosineSimilarity(value, centers[j]);
	    	if (result < minVal) {
	    		minVal = result;
	    		minPos = j;
	    	}
	    }
		
	    vec.copy(value);

	    word.set(String.valueOf(minPos));
	    
	    // send Team id, [vectors belonging to this team]
	    context.write(word, vec);
	    
	}
	
  }
  
  
  public static class PhaseTwoReduder 
  		extends Reducer<Text,VectorWritable,Text,VectorWritable> {
	  
	private VectorWritable vectormean;
	private int test =0;
	
	
	public void reduce(Text key, Iterable<VectorWritable> values, 
	                  Context context
	                  ) throws IOException, InterruptedException {
	
	    int count =0;
	    vectormean = new VectorWritable(N);
		
	    // calculate new center vector for this team
	    for(VectorWritable val : values)
		{
	    	vectormean.addvec(val);
			count++;
		}

		vectormean.division(count);
	
	    key.set("");
	    
	    // write new vector center
		context.write(key, vectormean);
	 
	}
	
  }
  
  // ---------------------------- End of Phase 2 -------------------------------------
  
  // ---------------------------------- Phase 3 --------------------------------------
  
  public static class MapperPhaseThree 
  		extends Mapper<Text, VectorWritable, IntWritable,Text>{

	private IntWritable x = new IntWritable();
	private Text word = new Text();
	private VectorWritable vec;
	private Path localFiles[];
	
	// calculate cosine similarity between vectors v1 and v2
	private float CosineSimilarity(VectorWritable v1, VectorWritable v2) {	
		
		float sum1 = 0, sum2 = 0, sum3 = 0;
		
		for (int i = 0; i < X; i++) {
			sum1 += v1.get(i)*v2.get(i);
			sum2 += v1.get(i)*v1.get(i);
			sum3 += v2.get(i)*v2.get(i);
		}
		sum2 = (float) Math.sqrt(sum2);
		sum3 = (float) Math.sqrt(sum3);
		
		return (1 - (sum1/(sum2*sum3)));
		
	}
	
	// get cache contents
	public void setup(Context context) {
		
		try {
			  localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			  localFiles[1] = localFiles[localFiles.length-1];
			  
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
    }
	

	public void map(Text key, VectorWritable value, Context context
	        ) throws IOException, InterruptedException {
		
		setup(context);
		
		String str;
		StringTokenizer itr;
		VectorWritable []centers = new VectorWritable[X];
	    int pos, i;  
		
	    // read team vector centers from cache one last time
	    BufferedReader readBuffer1 = new BufferedReader(new FileReader(localFiles[1].toString()));//diavazei apo tin cache
		String line;
		i=0;
	    while ((line=readBuffer1.readLine())!=null){
	    	
	    	pos = 0;
	    	centers[i] = new VectorWritable(N);
	    	itr = new StringTokenizer(line);
	    	while (itr.hasMoreTokens()) {
	    		centers[i].set(pos, Float.parseFloat(itr.nextToken()));
	    		pos++;
	    	}
	    	
	    	i++;
	    	
		}
		readBuffer1.close();
	    
	    // caclulate cosine similarity and assign word to a final team
	    float result;
	    int minPos = 0;
	    float minVal = CosineSimilarity(value, centers[0]);
	    for (i =1; i<X; i++){
	    	result = CosineSimilarity(value, centers[i]);
	    	if (result < minVal) {
	    		minVal = result;
	    		minPos = i;
	    	}
	    }
		
	    x.set(minPos);
	    
	    context.write(x, key);
			
	}
	
  }
  
  
  public static class ReducerPhaseThree 
  		extends Reducer<IntWritable,Text,IntWritable,Text> {
	  
	private  Text word;
	  
	public void reduce(IntWritable key, Iterable<Text> values, 
	                  Context context
	                  ) throws IOException, InterruptedException {
	 
		// write team id and word belonging to it
		int counter =0;
		for ( Text val : values) {
			counter++;
			context.write(key, val);
		}
		
		// write team id and number of elements in it
		Text dummy = new Text(Integer.toString(counter));
		context.write(key, dummy);
	 
	}
	
  }
  
  // ----------------------------- End of Phase 3 ---------------------------

  
  
  public static void main(String[] args) throws Exception { 
	  
	Scanner scanner = new Scanner(System.in);  
	System.out.println("Give number of k-means iterations to be performed: ");  
	int iter = scanner.nextInt();
	
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
    	System.err.println("Usage: wordcount <in> <out>");
    	System.exit(2);
    }

    //N must be initialized by user
    conf.setInt("N", N);
    
    Hash hash;
    
    
    // hdfs and path stuff
    FileSystem hdfs = FileSystem.get(new Configuration());
    
    // store stopwords.txt in distributed cache
    DistributedCache.addCacheFile(new URI(hdfs.getHomeDirectory() + "/project2/stopwords.txt"), conf);
    
    
    // FIRST MAP/REDUCE JOB - CREATE INVERTED INDEX
    Job job = new Job(conf, "word count");
    
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(VectorWritable.class);
    
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    SequenceFileAsBinaryOutputFormat.setOutputPath(job, new Path(hdfs.getHomeDirectory() + "/project2/temp")); // save index data in a temp directory
    
    job.waitForCompletion(true);
    

    
    // determine randomly X centers
    Random rand =new Random(System.currentTimeMillis());
    float temp;
    
    // create a file in hdfs to store means.txt before uploading to cache
    Path newFilePath=new Path(hdfs.getHomeDirectory() + "/project2/means.txt");
    hdfs.createNewFile(newFilePath);
      
    StringBuilder sb=new StringBuilder();

    for (int i =0; i<X;i++){
    	for(int j = 0 ; j<N ;j++ ){
    		temp =rand.nextFloat();
    		sb.append( Float.toString( temp) +" ");
    		
    	}
    	sb.append("\n");
    }

    byte[] byt=sb.toString().getBytes();

    FSDataOutputStream fsOutStream = hdfs.create(newFilePath);

    fsOutStream.write(byt);
    fsOutStream.close();
    
    
    // upload means.txt to distributed cache
    DistributedCache.addCacheFile(new URI(hdfs.getHomeDirectory() + "/project2/means.txt"), conf);

   
   
    // SECOND MAP/REDUCE JOBS - K-MEANS ALGORITHM
    Job job2 = new Job(conf, "word two");  
    
    for(int i=0; i<iter; i++)
    {
    	
    	if (i != 0) {
    		job2 = new Job(conf, "word two"); 
    	}
    	
    	job2.setJarByClass(WordCount.class);
        job2.setMapperClass(MapperPhaseTwo.class);
        job2.setReducerClass(PhaseTwoReduder.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(VectorWritable.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(NullWritable.class);
        
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job2, new Path(hdfs.getHomeDirectory() + "/project2/temp")); // input is the index
        SequenceFileAsBinaryOutputFormat.setOutputPath(job2, new Path(hdfs.getHomeDirectory() + "/project2/tempout2")); // save team vec centers in a tempout directory
        
        job2.waitForCompletion(true);
        
        // rename tempou2 folder to cache and then upload new centers file from cache folder to distributed cache
        if (i != 0) {
        	hdfs.delete(new Path(hdfs.getHomeDirectory() + "/project2/cache"),true);
        }
        
        hdfs.rename(new Path(hdfs.getHomeDirectory() + "/project2/tempout2"), new Path(hdfs.getHomeDirectory() + "/project2/cache"));
        hdfs.delete(new Path(hdfs.getHomeDirectory() + "/project2/cache/tempout2"),true);
        
        // upload file containing new vector centers in distributed cache
        DistributedCache.addCacheFile(new URI(hdfs.getHomeDirectory() + "/project2/cache/part-r-00000"), conf);      
        
        
        
    }
    

    
    // THIRD MAP/REDUCE JOB - RESULT PRESENTATION
    Job job3 = new Job(conf, "word two");
    
    job3.setJarByClass(WordCount.class);
    job3.setMapperClass(MapperPhaseThree.class);
    job3.setReducerClass(ReducerPhaseThree.class);
    job3.setOutputKeyClass(IntWritable.class);
    job3.setOutputValueClass(Text.class);
    
    job3.setInputFormatClass(SequenceFileInputFormat.class);

    FileInputFormat.addInputPath(job3, new Path(hdfs.getHomeDirectory() + "/project2/temp")); // inverted index as input
    //paizei na min thelei binary to katw
    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[1])); // produce final output file
    
    job3.waitForCompletion(true);
    
    
    
    // delete unwanted folders in HDFS
    hdfs.delete(new Path(hdfs.getHomeDirectory() + "/project2/cache"),true);
    hdfs.delete(new Path(hdfs.getHomeDirectory() + "/project2/means.txt"),true);
    
    
  }
  
}