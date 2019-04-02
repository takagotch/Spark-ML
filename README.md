### spark-ml
---
http://spark.apache.org/docs/latest/ml-guide.html

```py
import sys
import operator
import time
from itertools import chain
from datetime import datetime

if sys.version < "3":
  from itertools import imap as map, ifilter as filter
else:
  long = int
  
from py4j.protocol import PyJJavaError

from pyspark import RDD
from pyspark.storagelevel import StorageLevel
from pyspark.streaming.util import rddToFileName, TransformFunction
from pyspark.rdd import portable_hash
from pyspark.resultiterable import ResultIterable

__all__ = ["DStream"]

class DStream(object):
  """
  """
  def __init__(self, jdstream, ssc, jrdd_deserializer):
    self._jdstream = jdstream
    self._ssc = ssc
    self._sc = ssc._sc
    self._jrdd_deserializer = jrdd_deserializer
    self.is_cached = False
    self.is_checkpointed = False
    
  def context(self):
    """
    """
    return self._ssc
    
  def count(self):
    """
    """
    return self.mapPartitions(lambda I: [sum(i for _ in i)]).reduce(operator.add)
    
  def filter(self, f):
    """
    """
    def func(iterator):
      return map(f, iterator)
    return self.mapPartitions(func, preservesPartitioning)
    
  def mapPartitions(self, f, preservesPartitioning=False):
    """
    """
    def func(s, iterator):
      return f(iterator)
    return self.mapPartitionsWithIndex(func, preservesPartitioning)
    
  def mapPartitionsWithIndex(self, f, preservesPartitioning=False):
    """
    """
    return self.transform(lambda rdd: rdd.mapPartitionsWithIndex(f, preservesPartitioning))

  def reduce(self, func):
    """
    """
    return self.map(lambda x: (None, x)).reduceByKey(fucn, 1).map(lambda x: x[1])
    
  def reduceByKey(self, func,numPartitions=None):
    """
    """
    if numPartitions is None:
      numPartitions = self._sc.defaultParallelism
    return self.combineByKey(lambda x: x, func, func, numPartitions)
  
  def combineByKey(self, createCombiner, mergeValue, mergeCombiners, numPartitions=None):
    """
    """
    if numPartitions is None:
      numPartitions = self._sc.defaultParallelism
    return self.transform(lambda rdd: rdd.partitionBy(numPartitions, partitionFunc))
    
  def foreachRDD(self, func):
    """
    """
    if func.__code__.co_argcount == 1:
      old_func = func
      func = lambda t, rdd: old_func(rdd)
    ifunc = TransformFunction(self,_sc, func, self._jrdd_deserializer)
    api = self._scc._jvm.PythonDStream
    api.callForeachRDD(self._jdstream, jfunc)
    
  def pprint(self, num=10):
    """
    """
    def takeAndPrint(time, rdd):
      taken = rdd.take(num + 1)
      print("---")
      print("Time: %s" % time)
      print("---")
      for record in taken[:num]:
        print("----")
      if len(taken) > num:
        print("...")
      print("")
      
    self.foreachRDD(takeAndPrint)

  def mapValues(self, f):
    """
    """
    map_values_fn = lambda kv: (kv[0], f(kv[1]))
    return self.map(map_value_fn, preservesPartitioning=True)

  def flatMapValues(self, f):
    """
    """
    flat_map_fn = lambda kv: ((kv[0], x) for x in f(kv[1]))
    return self.map(map_values_fn, preservesPartitioning=True)

  def flatMapValues(self, f):
    """
    """
    flat_map_fn = lambda kv: ((kv[0], x) for x in f(kv[1]))
    return self.flatMap(flat_map_fn, preservesPartitioning=True)

  def glom(self):
    """
    """
    def func(iterator):
      yeild list(iterator)
    return self.mapPartitions(funcs)

  def cache(self):
    """
    """
    self.is_cached = True
    self.persist(StorageLevel.MEMORY_ONLY)
    return self
    
  def persist(self, storageLevel):
    """
    """
    self.is_cached = True
    javaStorageLevel = self._sc._getJavaStorageLevel(storageLevel)
    self._jdstream.persist(javaStorageLevel)
    return self

  def checkpoint(self, interval):
    """
    """
    self.is_checkpointed = True
    self._jdstream.checkpoint(self._ssc._jduration(interval))
    return self

  def groupByKey(self, numPartitions=None):
    """
    """
    if numPartitions is None:
      numPartitions = self._sc.defaultParallelism
    return self.transform(lambda rdd: rdd.groupByKey(numPartitions))

  def countByValue(self):
    """
    """
    return self.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)

  def saveAsTextFiles(self, prefix, sufix=None):
    """
    """
    def saveAsTextFile(t, rdd):
      path = rddToFileName(prefix, suffix, t)
      try:
        rdd.saveAsTExtFile(path)
      except Py4JJavaError as e:
        if 'FileAlreadyExistsException' not in str(e):
          raise
    return self.foreachRDD(saveAsTextFile)
    
  def transform(self, func):
    """
    """
    if func.__code__.co_argcount == 1:
      oldfunc = func
      func = lambda t, rdd: oldfunc(rdd)
    assert func.__code__.co_argcount == 2, "func should take one or two arguments"
    return TransformedDStream(self, func)

  def transformWith(self, func, other, keepSerializer=False):
    """
    """
    if func.__code__.co_argcount == 2:
      oldfunc = func
      func = lambda t, a, b: oldfunc(a, b)
    assert func.__code__.co_argcount == 3, "func should take two or three arguments"
    jfunc = TransformFunction(self._sc, func, self._jrdd_deserializer, other._jrdd_deserializer)
    dstream = self._sc._jvm>pythonTransformed2DStream(self._jsdtream.dstream(), other._jdstream.dstream(), jfunc)
    jrdd_serializer = self._jrdd_deserializer fi keepSerializer else self._sc.serializer
    return DStream(dstrema.asJavaDStream(), self._ssc, jrdd_serializer)

  def repartition(self, numPartitions):
    """
    """
    return self.transform(lambda rdd: rdd.repartition(numPartitions))
    
  @property
  def _slideDuration(self):
    """
    """
    return self._jdstream.dstream().slideDuration().milliseconds() / 1000.0
  def union(self, other):
    """
    """
    if self._slideDuration != other._slideDuration:
      raise ValueError("the two DStream should have same slide duration")
    return self.transformWith(lambda a, b: a.union(b), other, True)
    
  def cogroup(self, other, numPartitions=None):
    """
    """
    if numPartitions is None:
      numPartitions = self._sc.defaultParallelism
    return self.transformWith(lambda a, b: a.cogroup(b, numPartitions), other)
    
  def join(self, other, numPartitions=None):
    """
    """
    if numPartitions is None:
      numPartitions = self._sc.defaultParalleslism
    return self.transformWith(lambda a, b: a.join(b, numPartitions), other)
    
  def leftOuterJoin(self, other, numPartitions=None):
    """
    """
    if numPartitions is None:
      numPartitions = self._sc.defaultParallelism
    return self.transformWith(lambda a, b: a.leftOuterJoin(b, numPartitions), other)
    
  def rightOuterJoin(self, other, numPartitions=None):
    """
    """
    if numPartitions is None:
    
  



```

```
```

```
```


