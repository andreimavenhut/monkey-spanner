package spanner.monkey.hive

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorConverters, PrimitiveObjectInspector, ObjectInspector}
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException
import java.lang
import org.apache.commons.logging.LogFactory
import org.apache.commons.logging.Log


// sessionize(ts, gap, args... )
class GenericUDFSessionize extends GenericUDF {

  implicit def f(n: java.lang.Long): Long = Long2long(n)

  var LOG: Log = LogFactory.getLog(classOf[GenericUDFSessionize])
  var oi: PrimitiveObjectInspector = null
  @transient var tsConverter: ObjectInspectorConverters.Converter = null
  @transient var gapConverter: ObjectInspectorConverters.Converter = null
  @transient var argsOi: Array[PrimitiveObjectInspector] = null

  var currentMark: Array[Object] = null
  var currentStart: Long = -1
  var currentLast: Long = -1
  var gap: Long = 1800

  override def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {

    if (arguments.length < 3)
      throw new UDFArgumentLengthException("There should be at least 3 arguments")

    assert(arguments(0).isInstanceOf[PrimitiveObjectInspector])
    tsConverter = ObjectInspectorConverters.getConverter(
      arguments(0),
      PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG)
    )

    assert(arguments(1).isInstanceOf[PrimitiveObjectInspector])
    gapConverter = ObjectInspectorConverters.getConverter(
      arguments(1),
      PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.LONG)
    )

    argsOi = arguments.drop(2).map(arg => arg.asInstanceOf[PrimitiveObjectInspector])

    currentMark = new Array[Object](arguments.length - 2)

    PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
      PrimitiveObjectInspector.PrimitiveCategory.LONG
    )
  }

  def renewalMark(mark: Array[DeferredObject]) {
    for ((arg, i) <- mark.view.zipWithIndex)
      currentMark(i) = argsOi(i).getPrimitiveJavaObject(arg.get())
  }

  def isSameMark(mark: Array[DeferredObject]): Boolean = {
    (mark.view.zipWithIndex foldLeft (true)) {
      (isSame, e) => isSame &&
        argsOi(e._2).getPrimitiveJavaObject(e._1.get()).equals(currentMark(e._2))
    }
  }

  override def evaluate(arguments: Array[DeferredObject]): Object = {

    val ts = tsConverter.convert(arguments(0).get()).asInstanceOf[Long]

    // first record
    if (currentStart == -1) {
      currentStart = ts
      currentLast = ts
      gap = gapConverter.convert(arguments(1).get()).asInstanceOf[Long]
      LOG.info("initialized with gap=" + gap)
      renewalMark(arguments.drop(2))
    }
    else {
      if (isSameMark(arguments.drop(2))) {
        if (ts - currentLast >= gap) {
          // new session

          currentStart = ts
          currentLast = ts

        } else {
          currentLast = ts
        }
      } else {
        // another user
        currentStart = ts
        currentLast = ts
        renewalMark(arguments.drop(2))
      }
    }

    new lang.Long(currentStart)

  }

  override def getDisplayString(children: Array[String]): String = "sessionize"
}
