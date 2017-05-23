///////////////////////////////////////////////////////////////
//
//  This is a class runs through the folders in a directory and
//  stitches them together.  
//
//  Author:Sydney Lyon
//  Date Started: 7/7/2016
//  Last Worked: 7/18/2016
//
///////////////////////////////////////////////////////////////

import java.nio.file.Files;
import java.io.File;
import java.nio.file.StandardCopyOption;
import java.io.IOException;

public class FolderWalker{
  
  private String path;
  private String newPath;
  private String[] directory;
  private String[] files;
  private int size1, size2, filesCopied;
  //private FileWalker fileWalker;
  private String path1 = "";
  private String path2 = "";
  private String seperator = "";
    
  public FolderWalker(String path, String newPath){
    this.path = path;
    this.newPath = newPath;
    this.seperator = File.separator;
    directory = null;
    filesCopied = size1 = size2 = 0;
  }
  
  
  // gets all the file paths in a directory and stores them as a string in an arrayList
  public void getFolders(){
    File dir = new File(path);
    directory = dir.list();
    size1 = directory.length;
    
  }
  
  // opens all the folders that were collected in the parent folder
  public void openFolders(){
    final long startTime = System.nanoTime();
    for(String dir : directory){
      getFiles(path + seperator + dir);
      for(String fi : files){
        String old = path + seperator + dir + seperator + fi;
        copyFile(old);
        filesCopied++;
      }
    }
    final long copyTime = System.nanoTime() - startTime;
    System.out.println("Copying finished: took " + copyTime/1000000000 + " sec.");
  }
  
  // gets all the file paths in a directory and stores them as a string in an arrayList
  public void getFiles(String in){
    File fi = new File(in);
    files = fi.list();
    size2 = files.length;
  }

  // copies and renames file to new directory for stitching
  public void copyFile(String in){
    File source = new File(in);
    File dest = new File(makeNewPath(in));
    dest.getParentFile().mkdirs();
    try{
      Files.copy(source.toPath(), dest.toPath(),
                 StandardCopyOption.REPLACE_EXISTING);
    }catch(IOException e){
      e.printStackTrace();
    }
  }
  
  public String makeNewPath(String in){
    String tempPath = in;
    String out = "";
    out += newPath;
    String date = tempPath.substring(tempPath.length() - 15);
    String year = date.substring(1,5);
    String month = date.substring(6,8);
    String day = date.substring(date.length() - 6);
    // changes to the appropriate file name
    out += seperator + year + "-" + month + "-" + day;
    
    return out;
  }
  
  
  // creates output with details about copying
  public String testToString(){
    String out = "";
    out += "**********************************************\nFolder Tests\n";
    out += newPath + "\n";
    out += "filesCopied: " + filesCopied;
    out += "\n**********************************************";
    return out;
  }
  
  
  // runs necessary class functions
  public void run(){
    getFolders();
    openFolders();
    testToString();
    FileWalker fw = new FileWalker(newPath);
    fw.run();
    rename();
  }
  
  // to run if the files aren't properly renamed during stitching
  public void rename(){
    getFiles(newPath);
    System.out.println("-----------------------------------------------");
    for(String fi : files){
      String oldName = newPath + seperator + fi;
      if(oldName.substring(oldName.length() - 5).equals(".temp")){
        File temp = new File(oldName);
        String newName = oldName.substring(0, oldName.length() - 5);
        File perm = new File(newName);
        System.out.println("Rename this: " + oldName);
        if(!temp.renameTo(perm)){
          System.out.println("couldn't rename file");
        }else{
          System.out.println("To this: " + newName);
        }
        
      }else{
        System.out.println(oldName);
      }
    }
  }
  
  
  public static void main(String[] args){
    final long startTime = System.nanoTime();
    // in this new filewalker object, the first path is the folder you want to stitch together
    // the second path is where you want the stitched files to go
    //FolderWalker fw = new FolderWalker("/home/sylyon/2016","/home/sylyon/STITCHED");
    
    //C:\Users\Abhi\workspace\StitchingFlows\2016\
    
    FolderWalker fw = new FolderWalker("C:/Users/Abhi/workspace/StitchingFlows/2016",
            "C:/Users/Abhi/workspace/StitchingFlows/STITCHED");
    
    
    fw.run();
    //System.out.println(fw.getLastPath());
    final long duration = System.nanoTime() - startTime;
    System.out.println("" + duration/1000000000 + " sec");
  }
}
