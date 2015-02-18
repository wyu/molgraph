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
  protected PropertyGraph G=null;

  public TabReader(PropertyGraph g) { super(); G=g; };
}
