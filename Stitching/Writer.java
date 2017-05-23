///////////////////////////////////////////////////////////////
//
//  This is a class writes stitched results from array to a file
//
//  Author:Sydney Lyon
//  Date Started: 7/7/2016
//  Last Worked: 7/18/2016
//
///////////////////////////////////////////////////////////////

import java.util.ArrayList;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.PrintWriter;
import java.io.IOException;
import java.io.File;
import java.nio.file.Path;

public class Writer{
  private String path;
  private ArrayList<String[]> list;
  private static final String NEW_LINE = "\n";
  private static final String COMMA = ",";
  private int linesWritten, size;
  
    
  public Writer(String path, ArrayList<String[]> list){
    this.path = path;
    this.list = list;
    this.linesWritten = this.size = 0;
  }
  
  public void run(){
    write();
    System.out.println(testToString());
  }
  // creates output with details about reading
  public String testToString(){
    String out= "----------------------------------------------\nWriter Tests\n";
    out += path;
    out += "\nlistSize: " + size + "\nlinesWritten: " + linesWritten;
    out += "\n----------------------------------------------";
    return out;
  }
  
  public String arrayToString(String[] input){
    String[] in = input;
    String out = "";
    int size = in.length;
    for(int i=0; i<size; i++){
      if(i==0){
        out += in[i];
      }else{
        out += COMMA + in[i];
      }
    }
    out += NEW_LINE;
  
    return out;
  }
  
  
  public String listToString(){
    String out= "";
    int len = list.size();
    for(int i=0; i<len; i++){
      out += arrayToString(list.get(i));
      linesWritten++;
    }
    return out;
  }
  
  public void write(){
    size = list.size();
    String writeString = listToString();
    try(FileWriter fw = new FileWriter(path + ".temp", true);
        BufferedWriter bw = new BufferedWriter(fw);
        PrintWriter out = new PrintWriter(bw))
    {
      out.println(writeString);
      bw.flush();
      bw.close();
      fw.close();
      
    } catch (IOException e) {
        e.printStackTrace();
    }
    rename();
  }
  
  // renames the file
  public void rename(){
    File old = new File(path);
    File temp = new File(path + ".temp");
    old.delete();
    if(!temp.renameTo(old))
      System.out.println("Couldn't rename file");
  }
  
  public static void main(String[] args){
    //Reader test1 = new Reader("C:\\Users\\sylyon\\Documents\\Java\\Stitching\\01.csv");
    //ArrayList<String[]> testList = test1.run();
    //Writer test2 = new Writer("temp", testList);
    //System.out.println(test2.listToString());
  }
}