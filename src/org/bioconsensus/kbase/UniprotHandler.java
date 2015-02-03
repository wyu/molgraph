package org.bioconsensus.kbase;

import com.thinkaurelius.titan.core.*;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.utils.Strs;
import org.ms2ms.utils.Tools;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   12/11/14
 */
public class UniprotHandler extends TitanHandler
{
  public static final String ACC      = "accession";
  public static final String DB       = "db";
  public static final String TERM     = "term";
  public static final String PATHWAY  = "pathway name";
  public static final String MOLTYPE  = "molecule type";
  public static final String EVIDENCE = "evidence";
  public static final String ENTRY    = "entry name";
  public static final String PROTID   = "protein sequence ID";
  public static final String TYPE     = "type";
  public static final String SUBCELOC = "subcellularLocation";
  public static final String PROTEIN  = "protein";
  public static final String GENE     = "gene";

  PropertyNode protein, gene, primary_gene, dataset, loc, dbref, organism, interact;

  public UniprotHandler()                               { super(); }
  public UniprotHandler(TitanGraph graph)               { super(graph); }
  public UniprotHandler(TitanGraph graph, String... s)  { super(graph, s); }

  /** Clear the cache vars between the entries
   *
   */
  private void clearEntry()
  {
    protein=null; organism=null; gene=null; primary_gene=null; dataset=null; loc=null; dbref=null; organism=null; interact=null;
    _PROTEIN_ =null; _GENE_ =null; attrs=null; isParsing=false;
    name_acc.clear();
  }
  protected void prepareIndex()
  {
    prepareIndices(ACC,NAME,ORGANISM,SUBCELOC,LABEL,DB,TYPE,PROTEIN,GENE,TERM,PATHWAY,MOLTYPE,EVIDENCE,ENTRY,PROTID);
  }

  @Override
  public void startElement(String uri, String localName, String elementName, Attributes attributes) throws SAXException
  {
    stack.add(elementName); attrs = attributes;

    if      (elementName.equalsIgnoreCase("entry"))
    {
      // add to the graph. it's a new node by definition, assuming we're not re-import the same uniprot
      protein = newNode(PROTEIN, null, null);
      set(protein, "db", attributes.getValue("dataset"));
      dataset = newNode("db", attributes.getValue("dataset"));
//      if (entries%10000==0) System.out.println(entries+"$");
//      if (label_val_node.get("dataset", attributes.getValue("dataset"))!=null) dataset=titan.getVertexLabel("dataset");
    }
    else if (elementName.equalsIgnoreCase("dbReference"))
    {
      if (organism!=null && Strs.equals(Tools.fromLast(stack, 2), "organism") &&
          Strs.equals(attributes.getValue("type"), "NCBI Taxonomy"))
      {
        set(organism, attributes, "id"); organism.rename("id", "taxon");
      }
      if (Strs.equals(Tools.fromLast(stack, 2), "entry"))
      {
        dbref = newNode("dbref", attributes.getValue("id"));
        dbref.setProperty("db",  attributes.getValue("type"));
      }
    }
    else if (elementName.equalsIgnoreCase("property"))
    {
      if (Strs.equals(Tools.fromLast(stack, 2), "dbReference"))
      {
        dbref.setProperty(attributes.getValue("type"), attributes.getValue("value"));
      }
    }
    else if (elementName.equalsIgnoreCase("interactant"))
    {
      interact = newNode("interact", attributes.getValue("intactId"));
    }
    else if (Strs.isA(elementName, "fullName",ACC,"location","proteinExistence","name"))
    {
      isParsing=true; content=new StringBuilder();
    }
  }
  @Override
  public void endElement(String uri, String localName, String element) throws SAXException
  {
    if (!stack.removeLast().equals(element)) throw new RuntimeException("Unmatched element!");

    // if end of book element add to list
    if (element.equals("entry"))
    {
      // TODO deposit the protein and
      if (protein!=null && Strs.isA(protein.getProperty(ORGANISM), species))
      {
        if (++entries%1000==0) System.out.print(entries+".");

        // map the properties
        protein.rename(NAME,PROTEIN).rename("fullName", NAME);
        TitanVertex PROTEIN = getVertex(protein);
        if (PROTEIN!=null)
        {
          if (primary_gene!=null)
          {
            TitanVertex GENE=getVertex(primary_gene.getProperty("_label_"), primary_gene.getProperty("name"), primary_gene);
            addEdge(PROTEIN, GENE, "protein-gene");
            for (PropertyNode n : name_acc.get("geneid"))
              addEdge(getVertex(n), GENE, "id-gene");
          }
          // hookup the other accessions
          if (name_acc.get(ACC)!=null)
            for (PropertyNode n : name_acc.get(ACC))
              addEdge(getVertex(n), PROTEIN, "id-protein");
          // setup the dbrefs
          if (name_acc.get("dbref")!=null)
            for (PropertyNode n : name_acc.get("dbref"))
            {
              n.rename(DB, DB).rename("dbref", ACC);
              if (n.hasProperty(PROTID))
              {
                PropertyNode n1 = newNode("dbref", null,null);
                n1.setProperty(DB, n.getProperty("db"));
                n1.setProperty(ACC,n.getProperty(PROTID));
                addEdge(getVertex(n1.getProperty(LABEL), n1.getProperty(ACC), n1), PROTEIN, "dbref-protein");
              }
              addEdge(getVertex(n.getProperty(LABEL), n.getProperty(ACC), n), PROTEIN, "dbref-protein");
            }
        }
      }
      // clear out the record
      clearEntry();
    }
    else if (Strs.equals(element, "proteinExistence") && Strs.isSet(content))
    {
      set(protein, "existence", content);
    }
    else if (Strs.equals(element,"location") &&
             Strs.equals(stack.getLast(), SUBCELOC))
    {
      set(protein, SUBCELOC, content);
//      addEdge(loc, protein, "loc-protein");
    }
    else if (Strs.equals(element,ACC))
    {
      // setup the accession nodes that point up to the protein node
      PropertyNode n = newNode(ACC, content.toString());
      n.setProperty(DB, dataset.getProperty(DB));
//      n.rename("accession", "id");
      name_acc.put(ACC, n);
    }
    else if (Strs.equals(element, "fullName") &&
             Strs.equals(stack.getLast(), "recommendedName") && protein!=null)
    {
      set(protein, "fullName", content);
    }
    else if (element.equals("dbReference"))
    {
      if (dbref!=null) { name_acc.put("dbref", dbref); dbref=null; }
    }
    else if (element.equals(ORGANISM) && organism!=null && organism.getProperty("scientific")!=null)
    {
        protein.setProperty(ORGANISM, organism.getProperty("scientific"));
    }
    else if (Strs.equals(element, NAME))
    {
      if (Strs.equals(stack.getLast(), GENE))
      {
        gene = newNode("geneid", content.toString(), attrs, TYPE);
        gene.rename(   "geneid", ACC);
        name_acc.put(  "geneid", gene);
        // the primary gene which is usually defined by organization such as HUGO or MGD
        // primary, synonym, ORF
        if (primary_gene==null && Strs.equals(attrs.getValue(TYPE), "primary"))
        {
          primary_gene = gene.clone();
          primary_gene.setProperty(LABEL, GENE);
          primary_gene.getProperties().remove(TYPE);
          primary_gene.rename(ACC,NAME);
        }
      }
      else if (Strs.equals(stack.getLast(), "entry"))
      {
        set(protein, NAME, content);
      }
      else if (Strs.equals(stack.getLast(), ORGANISM))
      {
        if (organism==null) organism = new PropertyNode();
        set(organism, attrs.getValue(TYPE), content);
      }
    }
    isParsing=false;
  }
}