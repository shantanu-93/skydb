package hash;

/**  StringKey: It extends the KeyClass.
 *   It defines the string Key.
 */ 
public class StringKey implements KeyClass {

  private String key;

  public String toString(){
     return key;
  }

  /** Class constructor
   *  @param     s   the value of the string key to be set 
   */
  public StringKey(String s) { key = new String(s); }

  /** get a copy of the istring key
  *  @return the reference of the copy 
  */ 
  public int getKey() {return 0;}

  /** set the string key value
   */ 
  public void setKey(String s) { key=new String(s);}
}
