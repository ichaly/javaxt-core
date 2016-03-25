package javaxt.html;
import javaxt.xml.DOM;

public class Element {

    private String tagName;
    private String tagHTML;

    protected String innerHTML;
    protected String outerHTML;
    private boolean isStartTag;
    private boolean isEndTag;

    private String tag;
    private String[] arr;


  //**************************************************************************
  //** Constructor
  //**************************************************************************
  /**
   *  @param html HTML used to define a tag (e.g. &lt;div id="1"&gt;)
   */
    protected Element(String tagHTML){
        this.tagHTML = tagHTML;

        tag = tagHTML;

        if (tag.startsWith("</")){
            isStartTag = false;
            isEndTag = true;
        }
        else{
            isStartTag = true;
            if (tag.endsWith("/>")){
                isEndTag = true;
            }
            else{
                isEndTag = false;
            }
        }


        tag = tag.replace("</","");
        tag = tag.replace("<","");
        tag = tag.replace("/>","");
        tag = tag.replace(">","");
        //tag = AddQuotes(tag);

        tag = tag.replaceAll("\\s+"," ").trim();
        tag = tag.trim();

        arr = tag.split(" ");

        tagName = arr[0];
    }


    public boolean isStartTag(){
        return isStartTag;
    }

    public boolean isEndTag(){
        return isEndTag;
    }

  //**************************************************************************
  //** getName
  //**************************************************************************
  /** Returns the element tag/node name. */

    public String getName(){
        return tagName;
    }

    protected String getTag(){
        return tagHTML;
    }
    
    public String getInnerHTML(){
        return innerHTML;
    }


    public String getOuterHTML(){
        return outerHTML;
    }


  //**************************************************************************
  //** getAttributeValue
  //**************************************************************************
  /** Returns the value for a given attribute. If no match is found, returns
   *  an empty string.
   */
    public String getAttributeValue(String attributeName){
        try{
            org.w3c.dom.Document XMLDoc = DOM.createDocument("<" + tag + "/>");
            org.w3c.dom.NamedNodeMap attr = XMLDoc.getFirstChild().getAttributes();
            return DOM.getAttributeValue(attr,attributeName);
        }
        catch(Exception e){
            try{
               return getAttributeValue2(tag, attributeName);
            }
            catch(Exception ex){
               return "";
            }
        }

    }




    private String getAttributeValue2(String tag, String attributeName){

        tag = tag.trim();

        if (tag.contains((CharSequence) " ")==false){
            return tag;
        }

        String orgTag = tag;
        tag = tag.substring(tag.indexOf(" "), tag.length()).trim();


        String tagName = orgTag + " ";
        tagName = tagName.substring(0, tagName.indexOf(" "));

/*
        if (tagName.equalsIgnoreCase("img")){
            System.out.println("IMGTAG = " + tag);
        }
        else{
            return "";
        }

*/


      //compress spaces
        String newTag = "";
        tag += " ";
        boolean skipChar = false;
        for (int i=0; i<tag.length()-1; i++){
             char ch = tag.charAt(i);
             if (ch==' ' && tag.charAt(i+1)==' '){
             }
             else{
                 newTag += ch;
             }
        }

        newTag = newTag.replace("= ", "=");
        newTag = newTag.replace(" =", "=");

        //System.out.println("newTag = " + newTag);System.out.println();

        newTag = " " + newTag + " ";
        //attributeName = attributeName.toLowerCase();
        for (int i=0; i<newTag.length(); i++){
             char ch = newTag.charAt(i);


             if (ch == '='){

                 String tmp = newTag.substring(0, i);
                 //System.out.println(tmp);

                 String AttrName = tmp.substring(tmp.lastIndexOf(" "), tmp.length()).trim();
                 String AttrValue = "";

                 //System.out.println("AttrName = " + AttrName);
                 if (AttrName.equalsIgnoreCase(attributeName)){

                     tmp = newTag.substring(i+1, newTag.length()).trim() + " ";

                     if (newTag.charAt(i+1)=='"'){
                         tmp = tmp.substring(1, tmp.length());
                         AttrValue = tmp.substring(0, tmp.indexOf("\""));
                     }
                     else if (newTag.charAt(i+1)=='\''){
                         tmp = tmp.substring(1, tmp.length());
                         AttrValue = tmp.substring(0, tmp.indexOf("'"));
                     }
                     else{
                         AttrValue = tmp.substring(0, tmp.indexOf(" "));
                     }

                     return AttrValue;

                 }


             }


        }

        return "";
    }



  //**************************************************************************
  //** getElementByTagName
  //**************************************************************************
  /** Returns an array of HTML Elements with given tag name.
   */
    public Element[] getElementsByTagName(String tagName){
        return new Parser(innerHTML).getElementsByTagName(tagName);
    }


  //**************************************************************************
  //** getElementByTagName
  //**************************************************************************
  /** Returns the first HTML Element with given tag name. Returns null if an
   *  element was not found.
   */
    public Element getElementByTagName(String tagName){
        return getElementByAttributes(tagName, null, null);
    }


  //**************************************************************************
  //** getElementByID
  //**************************************************************************
  /** Returns an HTML Element with given a id. Returns null if the element was
   *  not found.
   */
    public Element getElementByID(String id){
        return getElementByAttributes(null, "id", id);
    }


    public Element[] getElements(String tagName, String attributeName, String attributeValue){
        return new Parser(innerHTML).getElements(tagName, attributeName, attributeValue);
    }


  //**************************************************************************
  //** getElementByAttributes
  //**************************************************************************
  /** Returns the first HTML Element with given tag name and attribute. Returns
   *  null if an element was not found.
   */
    public Element getElementByAttributes(String tagName, String attributeName, String attributeValue){
        return new Parser(innerHTML).getElementByAttributes(tagName, attributeName, attributeValue);
    }

    public String stripHTMLTags(){
        return Parser.stripHTMLTags(innerHTML);
    }


  //**************************************************************************
  //** getImageLinks
  //**************************************************************************
  /** Returns a list of links to images. The links may include relative paths.
   *  Use the getAbsolutePath method to resolve the relative paths to a fully
   *  qualified url.
   */
    public String[] getImageLinks(){
        return new Parser(innerHTML).getImageLinks();
    }
    
    public String toString(){
        return outerHTML;
    }
}