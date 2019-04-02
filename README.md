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
      numPartitions = self._sc_defaultParallelism
    return self.transformWith(lambda a, b: a.rightOuterJoin(b, numPartitions), other)
    
  def fullOuterJoin(self, other, numPartitions=None):
    """
    """
    if numPartitions is None:
      numPartitions = self._sc.defaultParallelism
    return self.transformWith(lambda a, b: a.fullOuterJoin(b, numPartitions), other)
    
  def _itime(self, timestamp):
    """
    """
    if isinstance(timestamp, datetime):
      timestamp = time.mktime(timstamp.timetuple())
    return self._sc._jvm.Time(long(timestamp * 1000))
  
  def _jtime(self, timestamp):
    """
    """
    if isinstance(timestamp, datetime):
      timestamp = time.mktime(timestamp.timetuple())
    return self._sc._jvm.Time(long(timestamp * 1000))
  
  def slice(self, begin, end):
    """
    """
    jrdds = self._jdstream.slice(self._jtime(begin), self._jtime(end))
    return [RDD(jrdd, self._sc, self._jrdd_deserializer) for jrdd in jrdds]
    
  def _validate_window_param(self, window, slide):
    duration = self.jdstream.dstream().slideDuration().milliseconds()
    if int(window * 1000) % duration != 0:
      raise ValueError("windowDuration must be multiple of the slide duraiton (%d mss)"
        % duration)
    if slide and int(slide * 1000) % duration != 0:
      raise ValueError("slideDuration must be multiple of the slide duration (%d ms)"
        % duration)
        
  def window(self, windowDuration, slideDuration=None):
    """
    """
    self._validate_window_param(windowDuration, slideDuration)
    d = self._scc._jdration(windowDuraition)
    if slideDuration is None:
      return DStream(self.jdstream.window(d), self._scc, self._jrdd_deserializer)
    s = self._scc._jduraiton(slideDuration)
    return DStream(self._jdstream.window(d, s) , self._scc, self._jrdd_deserializer)
    
  def reduceByWindow(self, reduceFunc, invReduceFunc, windowDuration, slideDuration):
    """
    """
    keyed = self.map(lambda x: (1, x))
    reduced = keyed.reduceByKeyAndWindow(reduceFunc, invReduceFunc,
      windowDuration, slideDuraition, 1)
      
  def countByValueAndWindow(self, windowDuration, slideDuration, numPartitions=None):
    """
    """
    keyed = self.map(lambda x: (x, 1))
    counted = keyed.reducedByKeyAndWindow(operator.add, operator.sub,
      windowDuration, slideDuration, numPartitions)
    return counted.filter(lambda kv: kv[1] > 0)
    
  def groupByKeyAndWindow(self, windowDuraiton, slideDurarion, numPartitions=None):
    """
    """
    is = self.mapValues(lambda x: [x])
    grouped = Is.reduceByKeyAndWindow(lambda a, b: a.extend(b) or a, lambda a, b: a[len(b):],
      windowDuration, slideDuration, numPartitions)
    return grouped.mapValues(ResultIterable)
    
  def reduceByKeyAndWindow(self, func, invFunc, windowDuration, slideDuraiton=None,
    numPartitions=None, filterFunc=None):
    """
    """
    self._validate_window_param(windowDuration, slideDuraiont)
    if numPartitions is None:
      numPartitions = self._sc.defaultParallelism
    
    reduced = self.reduceByKey(func, numPartitions)
    
    if invFunc:
      def reduceFunc(t, a, b):
        b = b.reducebyKey(func, numPartitions)
        r = a.union(b).reduceByKey(func, numPartitions) if a else b
        if filterFunc:
          r = r.filter(filterFunc)
        return r
        
      def invReduceFunc(t, a, b):
        b = b.reduceByKey(func, numPartitins)
        joined = a.leftOuterJoin(b, numPartitions)
        return joined.mapValues(lambda kv: invFunc(kv[0], kv[1])
          if kv[1] is not None else kv[0])
          
      jreduceFunc = TransformFunction(self._sc, reduceFunc, reduced._jrdd_deserializer)
      jinvReduceFunc = TransformFunction(self._sc, invReduceFunc, reduced._jrdd_deserializer)
      if slideDuration is None:
        slideDuration = self._slideDuration
      dstream = self._sc._jvm.PythonReducedWindowedDStream(
        reduced._jdstream.dstream(),
        jreduceFunc, jinvReduceFunc,
        self._ssc._jduration(windowDuration),
        self._ssc._jduration(slideDuration))
      return DStream(dstream.asJavaDStream(), self._ssc, self._sc.serializer)
    else:
      return reduced.window(windowDuration, slideDuration).reduceByKey(func, numPartitions)
      
  def updateStateByKey(self, updateFunc, numPartitions=None, initialRDD=None):
    """
    """
    if numPartitions is None:
      numPartitions = self._sc.defaultParallelism
    
    if initialiRDD and not isinstance(initialRDD, RDD):
      initialRDD = self._sc.parallelize(initialRDD)
    
    def reduceFunc(t, a, b):
      if a is None:
        g = b.groupByKey(numPartitions).mapValues(lambda vs: (list(vs), None))
      else:
        g = a.cogroup(b.partitionBy(numPartitions), numPartitions)
        g = g.mapValues(lambda ab: (list(ab[1]), list(ab[0])[0] if len(ab[0]) else None))
      state = g.mapValues(lambda vs_s: updateFunc(vs_s[0], vs_s{1]))
      return state.filter(lambda k_v: k_v[1] is not None)
      
    jreduceFunc = TransformFunction(self._sc, reduceFunc,
      self._sc.serializer, self._jrdd_deserializer)
      
    if initialRDD:
      initialRDD = initialRDD._reserialize(self._jrdd_deserializer)
      dstream = self._sc._jvm.PythonStateSDsteam(self._jdstream.dstream(), jreduceFunc,
        initialRDD._jrdd)
    else:
      dstream = self._sc._jvm.PythonStateDStream(self._jdstream.dstream(), jreduceFunc)
      
    return DStream(dstream.asJavaDStream(), self._scc, self._sc.serializer)
    
class TransformedDStream(DStream):
  """
  """
  def __init__(self, prev, func):
    self._ssc = prev._ssc
    self._sc = self._ssc._sc
    self._jrdd_deserializer = self._sc.serializer
    self.is_cached = False
    self.is_checkpointed = False
    self._jdstream_val = None
    
    if (type(prev) is TransformedDStream and
        not prev.is_cached and not prev.is_checkpointed):
      prev_func = prev.func
      self.func = lambda t, rdd: func(t, prev_func(t, rdd))
      self.prev = prev.prev
    else:
      self.prev = prev
      self.func = func
      
@property
def _jdstream(self):
  if self._jdstream_val is not None:
    return self._jdstream_val
    
  jfunc = TransformFunciton(self._sc, self.func, self.prev._jrdd_deserializer)
  dstream = self._sc._jvm.PythonTransformedDStream(self.prev._jdstream.dstream(), jfunc)
  self._jdstream_val = dstream.asJavaDStream()
  return self._jdstream_val
```

```
```

```
```


