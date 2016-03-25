package javaxt.html;

//******************************************************************************
//**  HTML Parser
//******************************************************************************
/**
 *   A simple html parser used to extract blocks of html from a document.
 *
 ******************************************************************************/

public class Parser {
    
    private String html;


  //**************************************************************************
  //** Constructor
  //**************************************************************************
    
    public Parser(String html){
        this.html = html;
    }


  //**************************************************************************
  //** setHTML
  //**************************************************************************
  /** Used to reset the "scope" of the parser */
    
    public void setHTML(String html){
        this.html = html;
    }


  //**************************************************************************
  //** getAbsolutePath
  //**************************************************************************

    public static String getAbsolutePath(String RelPath, String url){
        
      //Check whether RelPath is actually relative
        try{
            new java.net.URL(RelPath);
            return RelPath;
        }
        catch(Exception e){}


        
      //Remove "./" prefix in the RelPath
        if (RelPath.length()>2){
            if (RelPath.substring(0,2).equals("./")){
                RelPath = RelPath.substring(2,RelPath.length());
            }
        }
        
        
        String[] arrRelPath = RelPath.split("/");

        try{
            java.net.URL URL = new java.net.URL(url);
            String urlBase = URL.getProtocol() + "://" + URL.getHost();
            
            //System.out.println(url);
            //System.out.println(URL.getPath());
            //System.out.print(urlBase);
            

          //Build Path
            String urlPath = "";
            String newPath = "";
            if (RelPath.substring(0,1).equals("/")){
                newPath = RelPath;
            }
            else{
            
                urlPath = "/";
                String dir = "";
                String[] arr = URL.getPath().split("/");
                for (int i=0; i<=(arr.length-arrRelPath.length); i++){
                     dir = arr[i];
                     if (dir.length()>0){

                         urlPath += dir + "/";
                     }
                }
                //System.out.println(urlPath);
                
                
              //This can be cleaned-up a bit...
                if (RelPath.substring(0,1).equals("/")){
                    newPath = RelPath.substring(1,RelPath.length());
                }
                else if (RelPath.substring(0,2).equals("./")){
                    newPath = RelPath.substring(2,RelPath.length());
                }
                else if (RelPath.substring(0,3).equals("../")){
                    newPath = RelPath.replace("../", "");
                }
                else{
                    newPath = RelPath;
                }
            }

            

            return urlBase + urlPath + newPath;
            
        
        }
        catch(Exception e){}
        return null;
    }


  //**************************************************************************
  //** getElementByTagName
  //**************************************************************************
  /** Returns an array of HTML Elements found in the HTML document with given
   *  tag name.
   */
    public Element[] getElementsByTagName(String tagName){
        String orgHTML = html;
        java.util.ArrayList<Element> elements = new java.util.ArrayList<Element>();
        Element e = getElementByTagName(tagName);
        while (e!=null){
            elements.add(e);
            int idx = html.indexOf(e.outerHTML);
            String a = html.substring(0, idx);
            String b = html.substring(idx+e.outerHTML.length());
            html = a + b;
            e = getElementByTagName(tagName);
        }
        
        html = orgHTML;
        return elements.toArray(new Element[elements.size()]);
    }



  //**************************************************************************
  //** getElementByTagName
  //**************************************************************************
  /** Returns an array of HTML Elements found in the HTML document with given
   *  tag name.
   */
    public Element[] getElements(String tagName, String attributeName, String attributeValue){
        String orgHTML = html;
        java.util.ArrayList<Element> elements = new java.util.ArrayList<Element>();
        Element e = getElementByAttributes(tagName, attributeName, attributeValue);
        while (e!=null){
            elements.add(e);
            int idx = html.indexOf(e.outerHTML);
            String a = html.substring(0, idx);
            String b = html.substring(idx+e.outerHTML.length());
            html = a + b;
            e = getElementByAttributes(tagName, attributeName, attributeValue);
        }

        html = orgHTML;
        return elements.toArray(new Element[elements.size()]);
    }



  //**************************************************************************
  //** getElementByTagName
  //**************************************************************************
  /** Returns the first HTML Element found in the HTML document with given tag
   *  name. Returns null if an element was not found.
   */
    public Element getElementByTagName(String tagName){
        return getElementByAttributes(tagName, null, null);
    }


  //**************************************************************************
  //** getElementByID
  //**************************************************************************
  /** Returns an HTML Element with a given id. Returns null if the element was 
   *  not found.
   */
    public Element getElementByID(String id){
        return getElementByAttributes(null, "id", id);
    }


  //**************************************************************************
  //** getElementByAttributes
  //**************************************************************************
  /** Returns the first HTML Element found in the HTML document with given tag
   *  name and attribute. Returns null if an element was not found.
   */
    public Element getElementByAttributes(String tagName, String attributeName, String attributeValue){
        String s = html + " ";
        String c = "";
        
        boolean concat = false;
        int absStart = 0;
        int absEnd = 0;
        int numStartTags = 0;
        int numEndTags = 0;
         
        int outerStart = 0;
        int outerEnd = 0;

        StringBuffer tag = new StringBuffer();
        Element Element = null;

        boolean insideQuote = false;
        boolean insideComment = false;
                 
        for (int i = 0; i < s.length(); i++){

            c = s.substring(i,i+1); 


          //If we find the start of an html element, start assembling the tag
            if (c.equals("<") && !insideQuote && !insideComment){
                concat = true;
                absEnd = i;
            }
              

          //Check whether we are inside or outside a quote
            if (c.equals("\"")) {
                if (!insideQuote) insideQuote = true;
                else insideQuote = false;
            }
            

          //Check whether we are inside or outside a javascript or css comment
            if (c.equals("/")) {
                if (insideComment){
                    insideComment = (s.substring(i-1,i+1).equals("*/"));
                }
                else{
                    insideComment = (s.substring(i,i+2).equals("/*"));
                }
            }



            if (concat==true){
                tag.append(c);
            }


              
          //If we find the end of an html element, analyze the tag
            if ((c.equals(">") && !insideQuote && !insideComment) && concat==true){

                concat = false;
                Element Tag = new Element(tag.toString());

                if (Element==null){
                    if (Tag.isStartTag()){
                        if (tagName==null || Tag.getName().equalsIgnoreCase(tagName)){ //<--Tag name is null when getElementByID

                            if (attributeName==null || Tag.getAttributeValue(attributeName).equalsIgnoreCase(attributeValue)){ //<--Filter by attributes!
                                absStart = i+1;
                                Element = Tag;
                                outerStart = absStart - tag.length();


                              //Special case for getElementByID
                                if (tagName==null) tagName = Tag.getName();


                              //Special case for tags that self terminate
                                if (Tag.isEndTag()){
                                    //Element.innerHTML = null;
                                    Element.outerHTML = tag.toString();
                                    return Element;
                                }
                                
                            }
                        }
                    }
                }
                else {

                    if (Tag.getName().equalsIgnoreCase(tagName)){

                        
                        if (Tag.isStartTag()) numStartTags +=1;
                        //if (!Tag.isStartTag()) numEndTags +=1;
                        if (Tag.isEndTag()) numEndTags +=1;


                      //This is all new!
                        boolean foundEnd = false;
                        if (Tag.isStartTag() && Tag.isEndTag()){
                            foundEnd = false;
                        }
                        else if(!Tag.isStartTag() && Tag.isEndTag()){
                            foundEnd = (numEndTags>numStartTags);
                        }
                        else{
                            //System.out.println(numEndTags + " vs " + numStartTags);
                            foundEnd = (numEndTags>=numStartTags);
                        }



                        if (foundEnd){ // if (numEndTags>=numStartTags){
                            Element.innerHTML = html.substring(absStart,absEnd);
                            outerEnd = i+1;
                            Element.outerHTML = html.substring(outerStart,outerEnd);
                            return Element;
                        }
                    }


                }

                 
              //Clear tag variable for the next pass
                tag = new StringBuffer();
            }


        }


      //Last ditch effort!
        if (Element!=null){
            if (Element.getOuterHTML()==null){
                Element.outerHTML = Element.getTag();
            }
            return Element;
        }

        return null;
    }


  //**************************************************************************
  //** getImageLinks
  //**************************************************************************
  /** Returns a list of links to images. The links may include relative paths.
   *  Use the getAbsolutePath method to resolve the relative paths to a fully
   *  qualified url.
   */
    public String[] getImageLinks(){
        java.util.ArrayList<String> links = new java.util.ArrayList<String>();
        for (Element img : getElementsByTagName("img")){
            String src = img.getAttributeValue("src");
            if (src.length()>0) links.add(src);
        }
        return links.toArray(new String[links.size()]);
    }


  //**************************************************************************
  //** stripHTMLTags
  //**************************************************************************
  /**  Used to remove any html tags from a block of text */
    
    public static String stripHTMLTags(String html){
        
        String s = html + " ";
        String c = "";
        boolean concat = false;   
        String tag = "";
                 
        for (int i = 0; i < s.length(); i++){
              
             c = s.substring(i,i+1); 
               
             if (c.equals("<")){       
                 concat = true;
             }
              
             
             if (concat==true){
                 tag += c;
             } 
              
              
             if (c.equals(">") && concat==true){    
                 concat = false;
                 
                 html = html.replace(tag,"");
                         
               //Clear tag variable for the next pass
                 tag = "";     
             }
   
        }

        //html = html.replaceAll("\\s+"," ");
        
        return html.replace("&nbsp;", " ").trim();
    }
}