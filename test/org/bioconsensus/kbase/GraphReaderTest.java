package org.bioconsensus.kbase;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.junit.Assert;
import org.junit.Test;
import org.ms2ms.graph.Graphs;
import org.ms2ms.graph.PropertyEdge;
import org.ms2ms.graph.PropertyNode;
import org.ms2ms.nosql.Titans;
import org.ms2ms.r.Dataframe;
import org.ms2ms.test.TestAbstract;
import psidev.psi.mi.xml.PsimiXmlLightweightReader;
import psidev.psi.mi.xml.io.impl.PsimiXmlReader254;
import psidev.psi.mi.xml.model.Entry;
import psidev.psi.mi.xml.model.EntrySet;
import psidev.psi.mi.xml.xmlindex.IndexedEntry;
import psidev.psi.mi.xml.xmlindex.impl.PsimiXmlPullParser253;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   1/30/15
 */
public class GraphReaderTest extends TestAbstract
{
  @Test
  public void readComboGraph() throws Exception
  {
    String ibd = "/media/data/import/IntAct/psi25/datasets/IBD",
        dataset = "/media/data/import/IntAct/psi25/datasets",
        psi25 = "/media/data/import/IntAct/psi25";

    PsiMI25Reader interact = PsiMI25Reader.readRecursive(dataset);
    PropertyGraph.fixup(interact.G, new Dataframe("/media/data/import/HGNC_20150221.mapping", '\t'));

    System.out.println(interact.G.inventory());

    GWASReader g = new GWASReader(interact.G);
//    g.parseDocument("/home/wyu/Projects/molgraph/data/gwas_ebi1000.tsv");
    g.parseDocument("/media/data/import/GWAS/gwas_catalog_v1.0.1-downloaded_2015-04-08.tsv");
    System.out.println(g.G.inventory());

//    BioGRIDReader biogrid = new BioGRIDReader(g.G);
//
//    biogrid.parseDocuments("/media/data/import/BioGRID/BIOGRID-ALL-3.2.120.psi25.xml");
//    System.out.println(biogrid.G.inventory());
//    biogrid.G.write("/tmp/BioGRID.20150408");

    interact.G.writeNodes2CSVByLabel("/usr/local/neo4j/current/import/Combined");
    interact.G.writeEdges2CSVByType("/usr/local/neo4j/current/import/Combined");
  }

  @Test
  public void getTTD() throws Exception
  {
    String trans = "/media/data/import/TTD";

    TTDReader g = new TTDReader(new PropertyGraph());
    g.parseDocument(trans);
    System.out.println(g.G.inventory());
  }
  @Test
  public void getEBI_GWAS() throws Exception
  {
    GWASReader g = new GWASReader(new PropertyGraph());
    g.parseDocument("/home/wyu/Projects/molgraph/data/gwas_ebi1000.tsv");
//    g.parseDocument("/media/data/import/GWAS/gwas_catalog_v1.0.1-downloaded_2015-04-08.tsv");
    System.out.println(g.G.inventory());

//    g.G.writeNodes2CSVByLabel("/usr/local/neo4j/current/import/GWAS");
//    g.G.writeEdges2CSVByType("/usr/local/neo4j/current/import/GWAS");
    g.G.writeNodes2CSV("/usr/local/neo4j/current/import/GWAS");
    g.G.writeEdges2CSV("/usr/local/neo4j/current/import/GWAS");
    //g.G.write("/tmp/GWAS.20150407");
  }

  @Test
  public void readDrugBankGraph() throws Exception
  {
    DrugBankReader dbank = new DrugBankReader();
    dbank.parseDocument("/media/data/import/drugbank/drugbank.xml");
    System.out.println(dbank.G.inventory());
  }

  @Test
  public void readBioGRIDGraph() throws Exception
  {
//    PsiMI25Reader biogrid = GraphHandler.read("/media/data/import/BioGRID/BIOGRID-ALL-3.2.120.psi25.xml");
//    uniprot.parseDocument("/media/data/import/bio4j/uniprot_sprot.xml");
    BioGRIDReader biogrid = new BioGRIDReader(new PropertyGraph());

    biogrid.parseDocuments("/media/data/import/BioGRID/BIOGRID-ALL-3.2.120.psi25.xml");
    System.out.println(biogrid.G.inventory());
    biogrid.G.write("/tmp/BioGRID.20150408");
  }
  @Test
  public void readIntActGraph() throws Exception
  {
    PsiMI25Reader interact = PsiMI25Reader.read(null, "/home/wyu/Projects/molgraph/data/IBD25407307.xml");
  }
  @Test
  public void getInteractionGraph() throws Exception
  {
    String ibd = "/media/data/import/IntAct/psi25/datasets/IBD",
       dataset = "/media/data/import/IntAct/psi25/datasets",
         psi25 = "/media/data/import/IntAct/psi25";

    PsiMI25Reader interact = PsiMI25Reader.readRecursive(dataset);
    PropertyGraph.fixup(interact.G, new Dataframe("/media/data/import/HGNC_20150221.mapping", '\t'));

    interact.G.write("/tmp/IBD02");
    System.out.println(interact.G.inventory());

    interact.G.writeNodes2CSVByLabel("/usr/local/neo4j/current/import/IntAct");
    interact.G.writeEdges2CSVByType("/usr/local/neo4j/current/import/IntAct");

//    PropertyGraph g2 = PropertyGraph.read("/tmp/IBD02");
//    System.out.println(g2.inventory());

//    IOs.write("/tmp/IBD01.grp", PropertyGraph.toBytes(interact.G));
//    PropertyGraph g2 = PropertyGraph.fromBytes(IOs.readBytes("/tmp/IBD01.grp"));
//    System.out.println(g2.inventory());
  }
  @Test
  public void getGeneNetworkNL() throws Exception
  {
    String trans = "2012-12-21-TransEQTLsFDR0.5.txt",
             cis = "2012-12-21-CisAssociationsProbeLevelFDR0.5.txt",
//            root = "/home/wyu/Projects/molgraph/data/";
            root = "/media/data/import/eQTL/genenetwork.nl/";

    GeneNetworkNLReader g = new GeneNetworkNLReader(new PropertyGraph());
    g.parseDocument(root+trans);
    System.out.println(g.G.inventory());

    g.parseDocument(root+cis);
    System.out.println(g.G.inventory());

    System.out.println();
  }
  /** prepare the databases before running the test  **/
  // /usr/local/hbase/4titan$ bin/start-hbase.sh
  // sudo /etc/init.d/elasticsearch start

  @Test
  public void readBioGRID() throws Exception
  {
    TitanGraph g = Titans.openHBaseGraph();
    g.shutdown(); TitanCleanup.clear(g);

    g = Titans.openHBaseGraph();
    Psi25Handler biogrid = new Psi25Handler(g);
    biogrid.parseDocument("/media/data/import/BioGRID/BIOGRID-ALL-3.2.120.psi25.xml");
//    uniprot.parseDocument("/media/data/import/bio4j/uniprot_sprot.xml");
    g.shutdown();
  }

  @Test
  public void readPSI25() throws Exception
  {
    File file = new File("/home/wyu/Projects/molgraph/data/10373512.xml");
    PsimiXmlReader254 reader = new PsimiXmlReader254();
    EntrySet es = reader.read( file );

    DirectedGraph<PropertyNode, PropertyEdge> graph = new SimpleDirectedWeightedGraph<>(PropertyEdge.class);
    Map<String, PropertyNode> tag_node = new HashMap<>();
    graph = Graphs.readPsiMI(graph, tag_node, es);
    Assert.assertNotNull(es);
  }
  @Test
  public void StreamParser_253_file() throws Exception
  {
    FileInputStream file = new FileInputStream(new File("/media/data/import/BioGRID/BIOGRID-ALL-3.2.120.psi25.xml"));
    PsimiXmlPullParser253 reader = new PsimiXmlPullParser253();

    for (int i=0; i<10; i++)
    {
      Entry E = reader.parseEntry(file);
      System.out.println(E.hasAttributes());
    }
  }

  @Test
  public void getIndexedEntries_253_file() throws Exception
  {
    File file = new File("/media/data/import/BioGRID/BIOGRID-ALL-3.2.120.psi25.xml");
    PsimiXmlLightweightReader reader = new PsimiXmlLightweightReader( file );

    final List<IndexedEntry> indexedEntries = reader.getIndexedEntries();
    for (IndexedEntry iE : indexedEntries)
    {
      Entry E = iE.unmarshalledEntry();
      System.out.println(E.hasAttributes());
    }
    Assert.assertNotNull(indexedEntries);
    Assert.assertEquals( 1, indexedEntries.size() );
  }

/*
  private static void parsePsiMI(String xmlFileName) throws Exception
  {
    // initialise default factories for reading and writing MITAB/PSI-MI XML files
    PsiJami.initialiseAllFactories();

    // reading MITAB and PSI-MI XML files

    // the option factory for reading files and other datasources
    MIDataSourceOptionFactory optionfactory = MIDataSourceOptionFactory.getInstance();
    // the datasource factory for reading MITAB/PSI-MI XML files and other datasources
    MIDataSourceFactory dataSourceFactory = MIDataSourceFactory.getInstance();

    // get default options for a file. It will identify if the file is MITAB or PSI-MI XML file and then it will load the appropriate options.
    // By default, the datasource will be streaming (only returns an iterator of interactions), and returns a source of Interaction objects.
    // The default options can be overridden using the optionfactory or by manually adding options listed in MitabDataSourceOptions or PsiXmlDataSourceOptions
    Map<String, Object> parsingOptions = optionfactory.getDefaultOptions(new File(fileName));

    InteractionStream interactionSource = null;
    InteractionWriter xmlInteractionWriter = null;
    InteractionWriter mitabInteractionWriter = null;
    try{
      // Get the stream of interactions knowing the default options for this file
      interactionSource = dataSourceFactory.
          getInteractionSourceWith(parsingOptions);

      // writing MITAB and PSI-XML files

      // the option factory for reading files and other datasources
      MIWriterOptionFactory optionwriterFactory = MIWriterOptionFactory.getInstance();
      // the interaction writer factory for writing MITAB/PSI-MI XML files. Other writers can be dynamically added to the interactionWriterFactory
      InteractionWriterFactory writerFactory = InteractionWriterFactory.getInstance();

      // get default options for writing MITAB file.
      // By default, the writer will be a MITAB 2.7 writer and it will write the header
      // The default options can be overridden using the optionWriterfactory or by manually adding options listed in
      // MitabWriterOptions
      Map<String, Object> mitabWritingOptions = optionwriterFactory.getDefaultMitabOptions(new File(mitabFileName));

      // get default options for writing PSI-MI XML file.
      // By default, the writer will be a PSI-MI XML 2.5.4 writer and it will write expanded PSI-MI XML
      // The default options can be overridden using the optionWriterfactory or by manually adding options listed in
      // PsiXmlWriterOptions
      Map<String, Object> xmlWritingOptions = optionwriterFactory.getDefaultXmlOptions(new File(xmlFileName));

      // Get the default MITAB writer
      mitabInteractionWriter = writerFactory.getInteractionWriterWith(mitabWritingOptions);
      // Get the default PSI-MI XML writer
      mitabInteractionWriter = writerFactory.getInteractionWriterWith(mitabWritingOptions);

      // parse the stream and write as we parse
      // the interactionSource can be null if the file is not recognized or the provided options are not matching any existing/registered datasources
      if (interactionSource != null){
        Iterator interactionIterator = interactionSource.getInteractionsIterator();

        // start the writers (write headers, etc.)
        mitabInteractionWriter.start();
        xmlInteractionWriter.start();

        while (interactionIterator.hasNext()){
          Interaction interaction = (Interaction)interactionIterator.next();

          // most of the interactions will have experimental data attached to them so they will be of type InteractionEvidence
          if (interaction instanceof InteractionEvidence){
            InteractionEvidence interactionEvidence = (InteractionEvidence)interaction;
            // process the interaction evidence
          }
          // modelled interactions are equivalent to abstractInteractions in PSI-MI XML 3.0. They are returned when the interaction is not an
          // experimental interaction but a 'modelled' one extracted from any experimental context
          else if (interaction instanceof ModelledInteraction){
            ModelledInteraction modelledInteraction = (ModelledInteraction)interaction;
            // process the modelled interaction
          }

          // write the interaction in MITAB and XML
          mitabInteractionWriter.write(interaction);
          xmlInteractionWriter.write(interaction);
        }

        // end the writers (write end tags, etc.)
        mitabInteractionWriter.end();
        xmlInteractionWriter.end();
      }
    }
    finally {
      // always close the opened interaction stream
      if (interactionSource != null){
        interactionSource.close();
      }
      // always close the opened interaction writers
      if (mitabInteractionWriter != null){
        mitabInteractionWriter.close();
      }
      if (xmlInteractionWriter != null){
        xmlInteractionWriter.close();
      }
    }
  }
*/
}
