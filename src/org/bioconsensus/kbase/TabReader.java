package org.bioconsensus.kbase;

/**
 * ** Copyright 2014-2015 ms2ms.org
 * <p/>
 * Description:
 * <p/>
 * Author: wyu
 * Date:   2/12/15
 */
abstract public class TabReader
{
  protected GraphCache G=null;

  public TabReader(GraphCache g) { super(); G=g; };
}
