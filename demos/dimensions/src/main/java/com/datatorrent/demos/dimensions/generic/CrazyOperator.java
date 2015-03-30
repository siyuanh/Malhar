package com.datatorrent.demos.dimensions.generic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import com.datatorrent.api.BaseOperator;

/**
 * 
 * A Crazy operator!
 * @param <T>
 * @param <Z>
 */
public class CrazyOperator<T extends BlockingQueue, Z extends Map<String, Number>, SUB extends T> extends BaseOperator
{
  private int intProp;
  private long longProp;
  private double doubleProp;
  private boolean booleanProp;

  private List<String> stringList;
  private Properties props;
  private Structured nested;
  private Map<String, Structured> map = new HashMap<String, Structured>();
  private String[] stringArray;
  private Color color;
  private Structured[] structuredArray;
  private T[] genericArray;
  private SUB subGeneric;
  private Map<String, List<Map<String, Number>>> nestedParameterizedType = new HashMap<String, List<Map<String, Number>>>();
  private Map<? extends Object, ? super Long> wildcardType = new HashMap<Object, Number>();
  private List<int[]> listofIntArray = new LinkedList<int[]>();
  private List<T> parameterizedTypeVariable = new LinkedList<T>();
  private Z genericType;
  private int[][] multiDimensionPrimitiveArray;
  private Structured[][] multiDimensionComplexArray;

  public int getIntProp()
  {
    return intProp;
  }

  public void setIntProp(int intProp)
  {
    this.intProp = intProp;
  }

  public long getLongProp()
  {
    return longProp;
  }

  public void setLongProp(long longProp)
  {
    this.longProp = longProp;
  }

  public double getDoubleProp()
  {
    return doubleProp;
  }

  public void setDoubleProp(double doubleProp)
  {
    this.doubleProp = doubleProp;
  }

  public List<String> getStringList()
  {
    return stringList;
  }

  public void setStringList(List<String> stringList)
  {
    this.stringList = stringList;
  }

  public Properties getProps()
  {
    return props;
  }

  public void setProps(Properties props)
  {
    this.props = props;
  }

  /**
   * Example of pojo property
   * @return
   */
  public Structured getNested()
  {
    return nested;
  }

  public void setNested(Structured n)
  {
    this.nested = n;
  }

  public Map<String, Structured> getMap()
  {
    return map;
  }

  public void setMap(Map<String, Structured> m)
  {
    this.map = m;
  }

  /**
   * Example of enum property
   * @return
   */
  public Color getColor()
  {
    return color;
  }

  public void setColor(Color color)
  {
    this.color = color;
  }

  public String[] getStringArray()
  {
    return stringArray;
  }

  /**
   * @return array of structured objects
   */
  public Structured[] getStructuredArray()
  {
    return structuredArray;
  }

  public void setStructuredArray(Structured[] structuredArray)
  {
    this.structuredArray = structuredArray;
  }

  /**
   * Example of an array of generic type
   * @return 
   */
  public T[] getGenericArray()
  {
    return genericArray;
  }

  public void setGenericArray(T[] genericArray)
  {
    this.genericArray = genericArray;
  }

  /**
   * Example of boolean property
   * @return
   */
  public boolean isBooleanProp()
  {
    return booleanProp;
  }

  public void setBooleanProp(boolean booleanProp)
  {
    this.booleanProp = booleanProp;
  }

  public Map<String, List<Map<String, Number>>> getNestedParameterizedType()
  {
    return nestedParameterizedType;
  }

  /**
   * Example of nested parameterized type property
   * @param nestedParameterizedType
   */
  public void setNestedParameterizedType(Map<String, List<Map<String, Number>>> nestedParameterizedType)
  {
    this.nestedParameterizedType = nestedParameterizedType;
  }

  /**
   * Example of wildcard type property
   * @return
   */
  public Map<? extends Object, ? super Long> getWildcardType()
  {
    return wildcardType;
  }

  public void setWildcardType(Map<? extends Object, ? super Long> wildcardType)
  {
    this.wildcardType = wildcardType;
  }

  /**
   * Example of generic type property
   * @return
   */
  public Z getGenericType()
  {
    return genericType;
  }

  public void setGenericType(Z genericType)
  {
    this.genericType = genericType;
  }

  /**
   * Example of multi-dimension array property
   * @return
   */
  public int[][] getMultiDimensionPrimitiveArray()
  {
    return multiDimensionPrimitiveArray;
  }

  public void setMultiDimensionPrimitiveArray(int[][] multiDimensionPrimitiveArray)
  {
    this.multiDimensionPrimitiveArray = multiDimensionPrimitiveArray;
  }

  public Structured[][] getMultiDimensionComplexArray()
  {
    return multiDimensionComplexArray;
  }

  public void setMultiDimensionComplexArray(Structured[][] multiDimensionComplexArray)
  {
    this.multiDimensionComplexArray = multiDimensionComplexArray;
  }

  public List<int[]> getListofIntArray()
  {
    return listofIntArray;
  }

  public void setListofIntArray(List<int[]> listofIntArray)
  {
    this.listofIntArray = listofIntArray;
  }

  public List<T> getParameterizedTypeVariable()
  {
    return parameterizedTypeVariable;
  }

  public void setParameterizedTypeVariable(List<T> parameterizedTypeVariable)
  {
    this.parameterizedTypeVariable = parameterizedTypeVariable;
  }

  public <CRAZY extends Callable<Map<String, String>>> void setCraziness(CRAZY property)
  {
  }

  public SUB getSubGeneric()
  {
    return subGeneric;
  }

  public void setSubGeneric(SUB subGeneric)
  {
    this.subGeneric = subGeneric;
  }

  public static class Structured
  {
    private int size;
    private String name;
    private ArrayList<String> list;

    public int getSize()
    {
      return size;
    }

    public void setSize(int size)
    {
      this.size = size;
    }

    public String getName()
    {
      return name;
    }

    public void setName(String name)
    {
      this.name = name;
    }

    public ArrayList<String> getList()
    {
      return list;
    }

    public void setList(ArrayList<String> list)
    {
      this.list = list;
    }

  }

  public enum Color {
    BLUE, RED, WHITE
  }

}
