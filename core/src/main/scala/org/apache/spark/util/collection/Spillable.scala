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

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.memory.{MemoryConsumer, MemoryMode, TaskMemoryManager}

/**
 * 当超过内存阈值的时候将内存数据溢写到磁盘
 * 两个重要的实现类: ExternalAppendOnlyMap 和 ExternalSorter
 */
private[spark] abstract class Spillable[C](taskMemoryManager: TaskMemoryManager)
  extends MemoryConsumer(taskMemoryManager) with Logging {
  // 溢写
  protected def spill(collection: C): Unit

  // 强制溢写
  protected def forceSpill(): Boolean

  // 上次溢写后从上游读取的数据条数
  protected def elementsRead: Int = _elementsRead

  // Called by subclasses every time a record is read
  // It's used for checking spilling frequency
  protected def addElementsRead(): Unit = {
    _elementsRead += 1
  }

  // 初始化内存集合的大小， 仅供测试
  private[this] val initialMemoryThreshold: Long =
    SparkEnv.get.conf.get(SHUFFLE_SPILL_INITIAL_MEM_THRESHOLD)

  // 当内存中有这么多条数据时强制溢写， 仅供测试
  private[this] val numElementsForceSpillThreshold: Int =
    SparkEnv.get.conf.get(SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD)

  // 内存集合大小阈值，初始化>0减少大量小文件的溢写
  @volatile private[this] var myMemoryThreshold = initialMemoryThreshold

  // 上次溢写后从上游读取的数据条数
  private[this] var _elementsRead = 0

  // 总的溢写大小
  @volatile private[this] var _memoryBytesSpilled = 0L

  // 溢写次数
  private[this] var _spillCount = 0

  /**
   * 如果有必要将当前内存集合的数据溢写到磁盘，并在溢写前尝试去获取更多的资源
   *
   * @return 如果溢写成功返回true否则是false
   */
  protected def maybeSpill(collection: C, currentMemory: Long): Boolean = {
    var shouldSpill = false
    // 每读32条数据就判断一次，如果当前集合的大小超过了内存阈值，就去申请更多的内存
    if (elementsRead % 32 == 0 && currentMemory >= myMemoryThreshold) {
      // Claim up to double our current memory from the shuffle memory pool
      val amountToRequest = 2 * currentMemory - myMemoryThreshold
      val granted = acquireMemory(amountToRequest)
      myMemoryThreshold += granted
      // 如果申请到内存，就继续将数据放在内存结合中，如何没有，进一步判断是否溢写
      shouldSpill = currentMemory >= myMemoryThreshold
    }
    // 要判断数据大小和数据条数是否都超过阈值，都超过就开始溢写
    shouldSpill = shouldSpill || _elementsRead > numElementsForceSpillThreshold
    // Actually spill
    if (shouldSpill) {
      _spillCount += 1 // 溢写次数
      logSpillage(currentMemory)
      spill(collection) // 真正溢写
      _elementsRead = 0
      _memoryBytesSpilled += currentMemory // 统计总的溢写大小
      releaseMemory() // 释放资源回资源池
    }
    shouldSpill
  }

  /**
   * 将数据溢写到磁盘来释放资源，一般来说在没有充足的内存的时候被TaskMemoryManager调用
   */
  override def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger != this && taskMemoryManager.getTungstenMemoryMode == MemoryMode.ON_HEAP) {
      val isSpilled = forceSpill()
      if (!isSpilled) {
        0L
      } else {
        val freeMemory = myMemoryThreshold - initialMemoryThreshold
        _memoryBytesSpilled += freeMemory
        releaseMemory()
        freeMemory
      }
    } else {
      0L
    }
  }

  def memoryBytesSpilled: Long = _memoryBytesSpilled // 总溢写大小

  /**
   * 释放资源到计算资源池(对应内存管理里面的execution部分)供其他task获取
   */
  def releaseMemory(): Unit = {
    freeMemory(myMemoryThreshold - initialMemoryThreshold)
    myMemoryThreshold = initialMemoryThreshold
  }

  /**
   * 打印详细的溢写日志
   *
   * @param size 溢写的大小
   */
  @inline private def logSpillage(size: Long): Unit = {
    val threadId = Thread.currentThread().getId
    logInfo("Thread %d spilling in-memory map of %s to disk (%d time%s so far)"
      .format(threadId, org.apache.spark.util.Utils.bytesToString(size),
        _spillCount, if (_spillCount > 1) "s" else ""))
  }
}
