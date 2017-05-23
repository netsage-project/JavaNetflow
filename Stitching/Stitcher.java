///////////////////////////////////////////////////////////////
//
//  This is a class determine if two flows should be stitched
//  together
//
//  Author:Sydney Lyon
//  Date Started: 7/5/2016
//  Last Worked: 7/18/2016
//
///////////////////////////////////////////////////////////////
import java.text.DecimalFormat;
import java.util.ArrayList;

public class Stitcher{
  
  private ArrayList<String[]> firstList;
  private ArrayList<String[]> secondList;
  private Reader readerOne, readerTwo = null;
  private Writer wr;
  // for CSV
  private static final String COMMA = ",";
  private static final String NEW_LINE = "\n";
  private String pathOne, pathTwo;
  // test numbers
  int l1, l2, stitchedTotal;
    
  public Stitcher(String pathOne, String pathTwo){
    
    readerOne = new Reader(pathOne);
    readerTwo = new Reader(pathTwo);
    firstList = readerOne.run();
    secondList = readerTwo.run();
    this.pathOne = pathOne;
    this.pathTwo = pathTwo;
    stitchedTotal = l1 = l2 = 0;
  }
  
  // may need a seperate constructor that takes in an arrayList
  public Stitcher(ArrayList<String[]> firstList, String pathOne,
                  String pathTwo){
    readerTwo = new Reader(pathTwo);
    this.firstList = firstList;
    secondList = readerTwo.run();
    this.pathOne = pathOne;
    this.pathTwo = pathTwo;
    stitchedTotal = l1 = l2 = 0;
  }
  
  // compare contains two nested loops, based on our two arrayLists.  It iterates 
  // through and compares feilds from the coresponding String[] position with if 
  // statements to see if they match if statements are ordered based on how many flows
  // they rule out first int order to save computation in later if's
  public void compare(){
    //test numbers
    l1 = firstList.size();
    l2 = secondList.size();
    String out = "";
    for(int i=0; i<firstList.size(); i++){
      for(int j=0; j<secondList.size(); j++){
        if(firstList.get(i)[3].equals(secondList.get(j)[3])){ //sIP
          if(firstList.get(i)[5].equals(secondList.get(j)[5])){ //sPort
            if(firstList.get(i)[4].equals(secondList.get(j)[4])){ //dIP
              if(firstList.get(i)[6].equals(secondList.get(j)[6])){ //dPort
                if(firstList.get(i)[1].substring(0,9).
                     equals(secondList.get(j)[0].substring(0,9))){ // date
                  if(firstList.get(i)[7].equals(secondList.get(j)[7])){
                    stitchedTotal++;
                    secondList.get(j)[0] = firstList.get(i)[0];
                    // changes the byte total, adding both days in second's stats
                    secondList.get(j)[2]=addBytes(firstList.get(i),secondList.get(j),2);
                    secondList.get(j)[11] = addBytes(firstList.get(i),secondList.get(j),11);
                    secondList.get(j)[12] = addBytes(firstList.get(i),secondList.get(j),12);
                  
                    // takes out the now duplicate flow from first day
                    firstList.remove(i);
                    // moves index back so we don't miss any
                    if(i != 0)i--;
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  
  public void run(){
    // run the comparison of our two arrayLists created by Reader class
    this.compare();
    // write the updated flow records into the file
    wr = new Writer(pathOne, firstList);
    wr.write();
  }
  
  public ArrayList<String[]> getList(){
    return secondList;
  }
  
  // adds together the bytes column of our flow arrays
  public String addBytes(String[] a1, String[] a2,int index){
    String out = "";
    double total = Double.parseDouble(a1[index]) + Double.parseDouble(a2[index]);
    DecimalFormat df = new DecimalFormat("#");
    df.setMaximumFractionDigits(0);
    out += df.format(total);
    return out;
  }
  // creates output with details about stitching
  public String testToString(){
    if(readerOne != null)System.out.println(readerOne.testToString());
    System.out.println(readerTwo.testToString());
    //System.out.println(writerOne.testToString());
    String out = "";
    out += "**********************************************\nStitcher Tests\n";
    out += pathOne + "\n" + pathTwo + "\n";
    out += "listOneLength: " + l1 + "\nlistTwoLength: " + l2 + "\nstitchedTotal: " + stitchedTotal;
    out += "\n**********************************************";
    return out;
  }
  public Writer getWriter(){
    return wr;
  }
    
  public static void main(String[] args){
    
//    final long startTime = System.nanoTime();
//    Stitcher test = new Stitcher
//      ("C:\\Users\\sylyon\\Documents\\Java\\Stitching\\01.csv",
//       "C:\\Users\\sylyon\\Documents\\Java\\Stitching\\02.csv"
//       "C:\\Users\\sylyon\\Documents\\STITCHED);
//    
//    test.run();
//    System.out.println(test.testToString());
//    
//    final long duration = System.nanoTime() - startTime;
//    System.out.println("" + duration/1000000000 + " sec");
    
  }

}

