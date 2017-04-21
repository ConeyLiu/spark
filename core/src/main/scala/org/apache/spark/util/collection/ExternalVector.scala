/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util.collection

import java.io.{BufferedInputStream, File, FileInputStream}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.google.common.io.ByteStreams

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.{DeserializationStream, Serializer}
import org.apache.spark.storage.{BlockId, DiskBlockObjectWriter}


private[spark] class ExternalVector[T: ClassTag](
    context: TaskContext,
    serializer: Serializer = SparkEnv.get.serializer)
  extends Spillable[SizeTrackingVector[T]](context.taskMemoryManager())
  with Serializable
  with Logging{

  private val conf = SparkEnv.get.conf
  private val blockManager = SparkEnv.get.blockManager
  private val diskBlockManager = blockManager.diskBlockManager
  private val serializerManager = SparkEnv.get.serializerManager
  private val serInstance = serializer.newInstance()

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val fileBufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  // Size of object batches when reading/writing from serializers.
  //
  // Objects are written in batches, with each batch using its own serialization stream. This
  // cuts down on the size of reference-tracking maps constructed when deserializing a stream.
  //
  // NOTE: Setting this too low can cause excessive copying when serializing, since some serializers
  // grow internal data structures by growing + copying every time the number of objects doubles.
  private val serializerBatchSize = conf.getLong("spark.shuffle.spill.batchSize", 10000)

  // Information about a spilled file. Includes sizes in bytes of "batches" written by the
  // serializer as we periodically reset its stream, as well as number of elements in each
  // partition, used to efficiently keep track of partitions when merging.
  private[this] case class SpilledFile(
                                        file: File,
                                        blockId: BlockId,
                                        serializerBatchSizes: Array[Long])

  private val spills = new ArrayBuffer[SpilledFile]
  private var inMemIterator: Iterator[T] = null
  @volatile private var buffer = new SizeTrackingVector[T]()
  private var collapsed = false

  // Peak size of the in-memory map observed so far, in bytes
  private var _peakMemoryUsedBytes: Long = 0L
  def peakMemoryUsedBytes: Long = _peakMemoryUsedBytes

  def +=(value: T): Unit = {
    var estimatedSize = 0L
    addElementsRead()
    buffer += value
    estimatedSize = buffer.estimateSize()
    if (estimatedSize > _peakMemoryUsedBytes) {
      _peakMemoryUsedBytes = estimatedSize
    }

    if(maybeSpill(buffer, estimatedSize)) {
      buffer = new SizeTrackingVector[T]()
    }
  }

  def insertAll(records: Iterator[T]): Unit = {
    while (records.hasNext) {
      this += records.next()
    }
  }


  /**
    * Spills the current in-memory collection to disk, and releases the memory.
    *
    * @param vector vector to spill to disk
    */
  override protected def spill(vector: SizeTrackingVector[T]): Unit = {
    val vectorIter = vector.iterator
    spills += spillMemoryIteratorToDisk(vectorIter)
  }

  /**
    * Spill the in-memory Iterator to a temporary file on disk.
    */
  private[this] def spillMemoryIteratorToDisk(inMemoryIterator: Iterator[T]): SpilledFile = {
    val (blockId, file) = diskBlockManager.createTempLocalBlock()

    // These variables are reset after each flush
    var objectsWritten: Long = 0
    val spillMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics
    val writer: DiskBlockObjectWriter =
      blockManager.getDiskWriter(blockId, file, serInstance, fileBufferSize, spillMetrics)

    // List of batch sizes (bytes) in the order they are written to disk
    val batchSizes = new ArrayBuffer[Long]

    // Flush the disk writer's contents to disk, and update relevant variables.
    // The writer is committed at the end of this process.
    def flush(): Unit = {
      val segment = writer.commitAndGet()
      batchSizes += segment.length
      objectsWritten = 0
    }

    var success = false
    try {
      while (inMemoryIterator.hasNext) {
        writer.write(inMemoryIterator.next())
        objectsWritten += 1

        if (objectsWritten == serializerBatchSize) {
          flush()
        }
      }
      if (objectsWritten > 0) {
        flush()
      } else {
        writer.revertPartialWritesAndClose()
      }
      success = true
    } finally {
      if (success) {
        writer.close()
      } else {
        // This code path only happens if an exception was thrown above before we set success;
        // close our stuff and let the exception be thrown further
        writer.revertPartialWritesAndClose()
        if (file.exists()) {
          if (!file.delete()) {
            logWarning(s"Error deleting ${file}")
          }
        }
      }
    }

    SpilledFile(file, blockId, batchSizes.toArray)
  }

  def iterator(): Iterator[T] = {
    assert(!collapsed, "Should't call iterator on a collapsed vector.")
    val spilledIterators = spills.map(new SpillFileReader(_).getIterator())
    val memoryIterator: Iterator[T] = {
      if (inMemIterator == null) {
        inMemIterator = new SpillableIterator(buffer.iterator)
      }
      inMemIterator
    }

    val sequenceIterators = spilledIterators ++ Seq(memoryIterator)
    sequenceIterators.iterator.flatten
  }

  /**
    * Manually release the resources.
    */
  def close(): Unit = {
    collapsed = true
    spills.foreach {spilledFile =>
      if (spilledFile.file.exists()) {
        spilledFile.file.delete()
      }
    }

    spills.clear()
    buffer = null
    releaseMemory()
  }



  /**
    * Force to spilling the current in-memory collection to disk to release memory,
    * It will be called by TaskMemoryManager when there is not enough memory for the task.
    */
  override protected def forceSpill(): Boolean = {
    if (inMemIterator != null) {
      val isSpilled = inMemIterator.asInstanceOf[SpillableIterator].spill()
      if (isSpilled) {
        buffer = null
      }
      isSpilled
    } else if (buffer.size > 0) {
      spill(buffer)
      buffer = new SizeTrackingVector[T]()
      true
    } else {
      false
    }
  }

  private[this] class SpillFileReader(spilledFile: SpilledFile) {
    // Serializer batch offsets; size will be batchSize.length + 1
    val batchOffsets = spilledFile.serializerBatchSizes.scanLeft(0L)(_ + _)

    var curBatchId = 0
    var curIndexInBatch = 0

    var fileStream: FileInputStream = null
    var deserializeStream: DeserializationStream = nextFileBatchStream()

    def nextFileBatchStream(): DeserializationStream = {
      if (curBatchId < (batchOffsets.length - 1)) {
        val start = batchOffsets(curBatchId)
        val end = batchOffsets(curBatchId + 1)

        assert(end >= start, "start = " + start + ", end = " + end +
          ", batchOffsets = " + batchOffsets.mkString("[", ", ", "]"))

        fileStream = new FileInputStream(spilledFile.file)
        fileStream.getChannel.position(start)

        val bufferedStream = new BufferedInputStream(ByteStreams.limit(fileStream, end - start))

        val wrappedStream = serializerManager.wrapStream(spilledFile.blockId, bufferedStream)
        serInstance.deserializeStream(wrappedStream)
      } else {
        cleanup()
        null
      }
    }

    def cleanup(): Unit = {
      if (fileStream != null) {
        fileStream.close()
        fileStream = null
      }

      if (deserializeStream != null) {
        deserializeStream.close()
        deserializeStream = null
      }
    }

    def getIterator(): Iterator[T] = {
      new Iterator[T] {
        override def hasNext: Boolean = {
          if (collapsed
            || curBatchId >= spilledFile.serializerBatchSizes.length
            || deserializeStream == null) {
            false
          } else {
            true
          }
        }

        override def next(): T = {
          if (!hasNext) {
            throw new NoSuchElementException
          }

          assert(deserializeStream != null, "There is no record can be read.")

          val record = deserializeStream.readObject()
          curIndexInBatch += 1
          if (curIndexInBatch == spilledFile.serializerBatchSizes(curBatchId)) {
            cleanup()
            curBatchId += 1
            deserializeStream = nextFileBatchStream()
            curIndexInBatch = 0
          }
          record.asInstanceOf[T]
        }
      }
    }
  }

  /**
    *  An iterator that supports spill.
    */
  private[this] class SpillableIterator(var upstream: Iterator[T])
    extends Iterator[T] {

    private val SPILL_LOCK = new Object()

    private var nextUpstream: Iterator[T] = null

    private var cur: T = readNext()

    private var hasSpilled: Boolean = false

    def spill(): Boolean = SPILL_LOCK.synchronized {
      if (hasSpilled) {
        false
      } else {
        logInfo(s"Task ${context.taskAttemptId} force spilling in-memory map to disk and " +
          s"it will release ${org.apache.spark.util.Utils.bytesToString(getUsed())} memory")
        val spilledFile = spillMemoryIteratorToDisk(upstream)
        nextUpstream = new SpillFileReader(spilledFile).getIterator()
        hasSpilled = true
        true
      }
    }

    def readNext(): T = SPILL_LOCK.synchronized {
      if (nextUpstream != null) {
        upstream = nextUpstream
        nextUpstream = null
      }
      if (upstream.hasNext) {
        upstream.next()
      } else {
        null.asInstanceOf[T]
      }
    }

    override def hasNext(): Boolean = cur != null

    override def next(): T = {
      val r = cur
      cur = readNext()
      r
    }
  }
}
