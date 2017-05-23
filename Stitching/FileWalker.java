///////////////////////////////////////////////////////////////
//
//  This is a class runs through the files in a directory and
//  stitches them together
//
//  Author:Sydney Lyon
//  Date Started: 7/7/2016
//  Last Worked: 7/18/2016
//
///////////////////////////////////////////////////////////////

import java.io.File;
import java.util.ArrayList;
import java.nio.file.Files;
  
public class FileWalker{
  
  private String path;
  private File[] directoryListing;
  private int size;
  private Stitcher st;
  private String path1 = "";
  private String path2 = "";
  private ArrayList<String[]> bucket;
  
  public FileWalker(String path){
    this.path = path;
    directoryListing = null;
    st = null;
    size = 0;
  }
  
  
  // gets all the file paths in a directory and stores them as a string in an arrayList
  public void getFiles(){
    File dir = new File(path);
    directoryListing = dir.listFiles();
    size = directoryListing.length;
  }
  
  public void stitchFiles(){
    // creates the array of absolute paths we are working with
    getFiles();
    // sets both paths to the empty string everytime the program is run
    
    // makes size one less, so that the loop stops at second to last
    // and last items in the array
    size--;
    // temporary holding for the list we are working on
    bucket = null;
    
    // this loop works by using two file paths in the array and creating
    // a new stitching object for them.
    for(int i=0; i<size; i++){
      
      if(i == 0){
        // special case for the first two, we will need two readers set up, so we use the
        // first stitcher constructor type
        path1 = directoryListing[i].getPath();
        path2 = directoryListing[i+1].getPath();
        st = new Stitcher(path1, path2);
        st.run();
        // stores the second list in our bucket so that we can use it in our else case
        bucket = st.getList();
        // prints results
        System.out.println(st.testToString());
      }else{
        // switches our day two to day one and gets next day's path to stitch them together
        path1 = path2;
        path2 = directoryListing[i+1].getPath();
        st = new Stitcher(bucket, path1, path2);
        st.run();
        // updates the bucket
        bucket = st.getList();
        // prints results
        System.out.println(st.testToString());
        System.out.println(st.getWriter().testToString());
      }
    }
    
    // writes the flows of the last stitch stitcher only writes the first file
    // so we can carry over the lists
    Writer wr = new Writer(path2, bucket);
    wr.write();
    System.out.println(wr.testToString());
    
    
  }
  
  public ArrayList<String[]> getBucket(){
    return bucket;
  }
  public String getFirstPath(){
    return directoryListing[0].getPath();
  }
  public String getLastPath(){
    return path2;
  }
  
  // prints the results of stitch testing and any other useful testing data
  public String testToString(){
    String out = "";
    out += st.testToString();
    return out;
  }
  
  
  // runs necessary class functions
  public void run(){
    stitchFiles();
  }
  
  public static void main(String[] args){
    final long startTime = System.nanoTime();
    // in this new filewalker object, the first path is the folder you want to stitch together
    // the second path is where you want the stitched files to go
    FileWalker fw = new FileWalker("C:\\Users\\sylyon\\Documents\\Java\\Stitching\\2016\\01");
    fw.run();
    System.out.println(fw.getLastPath());
    final long duration = System.nanoTime() - startTime;
    System.out.println("" + duration/1000000000 + " sec");
  }

}