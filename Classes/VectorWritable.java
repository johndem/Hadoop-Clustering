package test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class VectorWritable extends Configured implements Writable {

  private int n=12; // SET TO NUMBER OF DOCS
  private FloatWritable v[];

  
  public VectorWritable() {
	  v = new FloatWritable[n];
	  for(int i =0; i<n ;i++)
	  {
		  v[i] = new FloatWritable((float)0);
	  }
  }
  
  public VectorWritable(int n) {
	  this.n = n;
	  v = new FloatWritable[n];
	  for(int i =0; i<n ;i++)
	  {
		  v[i] = new FloatWritable((float)0);
	  }
  }
  
  // return vector
  public FloatWritable[] get() {
    return v;
  }
  
  // return a vector element
  public float get(int pos) {
	  return v[pos].get();
  }
  
  // return vector's size
  public int getSize() {
	  return n;
  }
  
  // copy vec's elements to vector
  public void copy(VectorWritable  vec)
  {
	  for(int i =0 ; i< n ; i++)
	  {
		  v[i].set(vec.get(i));
	  }
  }
  
  // add a vec's elements to vector's
  public void addvec(VectorWritable vec)
  {
	  for(int i =0 ;i< n ; i++)
	  {
		  v[i].set(v[i].get() + vec.get(i));
	  }
  }
  
  // divide vector's each element with count
  public void division(int count)
  {
	  for(int i =0 ;i< n ; i++)
	  {
		  v[i].set(v[i].get()/count);
	  }
  }
  
  // puts 1 at a specific position of vector
  public void set(int pos) {
	  v[pos].set((float)1);
  }
  
  // puts a specific value in given position of vector
  public void set(int pos, float value) {
	  v[pos].set(value);
  }
  
  @Override
  public String toString() {
	  StringBuilder str = new StringBuilder();
	  
	  for(int i=0;i<n ;i++)
	  {
		  str.append(v[i].get());
		  str.append(" ");
	  }
	  return str.toString();
  }
 
  // print vector's contents
  public void readVector(int N) {
	  System.out.println("VECTOR:");
	  for (int i = 0; i < N; i++) {
		  System.out.println(v[i]);
	  }
  }

  @Override
  public void write(DataOutput out) throws IOException {
	  for(int i=0 ; i<n ; i++)
	  {
		  v[i].write(out);
	  }
	  
  }

  @Override
  public void readFields(DataInput in) throws IOException {
	  
	  for(int i =0; i<n; i++)
	  {
		  if (in==null) System.err.println("NULL"); 
		  if (v[i]==null) System.err.println("NULL = V[I]"); 
		  v[i].readFields(in);
	  }
  }


}