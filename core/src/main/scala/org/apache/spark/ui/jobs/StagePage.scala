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

package org.apache.spark.ui.jobs

import java.net.URLEncoder
import java.util.Date
import javax.servlet.http.HttpServletRequest

import scala.collection.mutable.HashSet
import scala.xml.{Elem, Node, Unparsed}

import org.apache.commons.lang3.StringEscapeUtils

import org.apache.spark.SparkConf
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.{AccumulableInfo, TaskInfo, TaskLocality}
import org.apache.spark.ui._
import org.apache.spark.ui.jobs.UIData._
import org.apache.spark.util.{Distribution, Utils}

/** Page showing statistics and task list for a given stage */
private[ui] class StagePage(parent: StagesTab) extends WebUIPage("stage") {
  import StagePage._

  private val progressListener = parent.progressListener
  private val operationGraphListener = parent.operationGraphListener

  private val TIMELINE_LEGEND = {
    <div class="legend-area">
      <svg>
        {
          val legendPairs = List(("scheduler-delay-proportion", "Scheduler Delay"),
            ("deserialization-time-proportion", "Task Deserialization Time"),
            ("shuffle-read-time-proportion", "Shuffle Read Time"),
            ("executor-runtime-proportion", "Executor Computing Time"),
            ("shuffle-write-time-proportion", "Shuffle Write Time"),
            ("serialization-time-proportion", "Result Serialization Time"),
            ("getting-result-time-proportion", "Getting Result Time"))

          legendPairs.zipWithIndex.map {
            case ((classAttr, name), index) =>
              <rect x={5 + (index / 3) * 210 + "px"} y={10 + (index % 3) * 15 + "px"}
                width="10px" height="10px" class={classAttr}></rect>
                <text x={25 + (index / 3) * 210 + "px"}
                  y={20 + (index % 3) * 15 + "px"}>{name}</text>
          }
        }
      </svg>
    </div>
  }

  // TODO: We should consider increasing the number of this parameter over time
  // if we find that it's okay.
  private val MAX_TIMELINE_TASKS = parent.conf.getInt("spark.ui.timeline.tasks.maximum", 1000)

  private val displayPeakExecutionMemory = parent.conf.getBoolean("spark.sql.unsafe.enabled", true)

  private def getLocalitySummaryString(stageData: StageUIData): String = {
    val localities = stageData.taskData.values.map(_.taskInfo.taskLocality)
    val localityCounts = localities.groupBy(identity).mapValues(_.size)
    val localityNamesAndCounts = localityCounts.toSeq.map { case (locality, count) =>
      val localityName = locality match {
        case TaskLocality.PROCESS_LOCAL => "Process local"
        case TaskLocality.NODE_LOCAL => "Node local"
        case TaskLocality.RACK_LOCAL => "Rack local"
        case TaskLocality.ANY => "Any"
      }
      s"$localityName: $count"
    }
    localityNamesAndCounts.sorted.mkString("; ")
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    progressListener.synchronized {
      val parameterId = request.getParameter("id")
      require(parameterId != null && parameterId.nonEmpty, "Missing id parameter")

      val parameterAttempt = request.getParameter("attempt")
      require(parameterAttempt != null && parameterAttempt.nonEmpty, "Missing attempt parameter")

      val parameterTaskPage = request.getParameter("task.page")
      val parameterTaskSortColumn = request.getParameter("task.sort")
      val parameterTaskSortDesc = request.getParameter("task.desc")
      val parameterTaskPageSize = request.getParameter("task.pageSize")
      val parameterTaskPrevPageSize = request.getParameter("task.prevPageSize")

      val taskPage = Option(parameterTaskPage).map(_.toInt).getOrElse(1)
      val taskSortColumn = Option(parameterTaskSortColumn).map { sortColumn =>
        UIUtils.decodeURLParameter(sortColumn)
      }.getOrElse("序号")
      val taskSortDesc = Option(parameterTaskSortDesc).map(_.toBoolean).getOrElse(false)
      val taskPageSize = Option(parameterTaskPageSize).map(_.toInt).getOrElse(100)
      val taskPrevPageSize = Option(parameterTaskPrevPageSize).map(_.toInt).getOrElse(taskPageSize)

      val stageId = parameterId.toInt
      val stageAttemptId = parameterAttempt.toInt
      val stageDataOption = progressListener.stageIdToData.get((stageId, stageAttemptId))

      val stageHeader = s"阶段信息 $stageId (尝试 $stageAttemptId)"
      if (stageDataOption.isEmpty) {
        val content =
          <div id="no-info">
            <p>该阶段没有展示信息 {stageId} (尝试 {stageAttemptId})</p>
          </div>
        return UIUtils.headerSparkPage(stageHeader, content, parent)

      }
      if (stageDataOption.get.taskData.isEmpty) {
        val content =
          <div>
            <h4>监控摘要</h4> 没有开始的任务
            <h4>任务</h4> 没有开始的任务
          </div>
        return UIUtils.headerSparkPage(stageHeader, content, parent)
      }

      val stageData = stageDataOption.get
      val tasks = stageData.taskData.values.toSeq.sortBy(_.taskInfo.launchTime)
      val numCompleted = stageData.numCompleteTasks
      val totalTasks = stageData.numActiveTasks +
        stageData.numCompleteTasks + stageData.numFailedTasks
      val totalTasksNumStr = if (totalTasks == tasks.size) {
        s"$totalTasks"
      } else {
        s"$totalTasks, showing ${tasks.size}"
      }

      val allAccumulables = progressListener.stageIdToData((stageId, stageAttemptId)).accumulables
      val externalAccumulables = allAccumulables.values.filter { acc => !acc.internal }
      val hasAccumulators = externalAccumulables.size > 0

      val summary =
        <div>
          <ul class="unstyled">
            <li>
              <strong>全部任务运行时长: </strong>
              {UIUtils.formatDuration(stageData.executorRunTime)}
            </li>
            <li>
              <strong>本地级别摘要: </strong>
              {getLocalitySummaryString(stageData)}
            </li>
            {if (stageData.hasInput) {
              <li>
                <strong>输入大小/ 记录: </strong>
                {s"${Utils.bytesToString(stageData.inputBytes)} / ${stageData.inputRecords}"}
              </li>
            }}
            {if (stageData.hasOutput) {
              <li>
                <strong>输出: </strong>
                {s"${Utils.bytesToString(stageData.outputBytes)} / ${stageData.outputRecords}"}
              </li>
            }}
            {if (stageData.hasShuffleRead) {
              <li>
                <strong>Shuffle 读: </strong>
                {s"${Utils.bytesToString(stageData.shuffleReadTotalBytes)} / " +
                 s"${stageData.shuffleReadRecords}"}
              </li>
            }}
            {if (stageData.hasShuffleWrite) {
              <li>
                <strong>Shuffle 写: </strong>
                 {s"${Utils.bytesToString(stageData.shuffleWriteBytes)} / " +
                 s"${stageData.shuffleWriteRecords}"}
              </li>
            }}
            {if (stageData.hasBytesSpilled) {
              <li>
                <strong>Shuffle 溢出 (内存): </strong>
                {Utils.bytesToString(stageData.memoryBytesSpilled)}
              </li>
              <li>
                <strong>Shuffle 溢出 (磁盘): </strong>
                {Utils.bytesToString(stageData.diskBytesSpilled)}
              </li>
            }}
          </ul>
        </div>

      val showAdditionalMetrics =
        <div>
          <span class="expand-additional-metrics">
            <span class="expand-additional-metrics-arrow arrow-closed"></span>
            <a>显示附加监控</a>
          </span>
          <div class="additional-metrics collapsed">
            <ul>
              <li>
                  <input type="checkbox" id="select-all-metrics"/>
                  <span class="additional-metric-title"><em>全选</em></span>
              </li>
              <li>
                <span data-toggle="tooltip"
                      title={ToolTips.SCHEDULER_DELAY} data-placement="right">
                  <input type="checkbox" name={TaskDetailsClassNames.SCHEDULER_DELAY}/>
                  <span class="additional-metric-title">调度延迟</span>
                </span>
              </li>
              <li>
                <span data-toggle="tooltip"
                      title={ToolTips.TASK_DESERIALIZATION_TIME} data-placement="right">
                  <input type="checkbox" name={TaskDetailsClassNames.TASK_DESERIALIZATION_TIME}/>
                  <span class="additional-metric-title">任务反序列化时长</span>
                </span>
              </li>
              {if (stageData.hasShuffleRead) {
                <li>
                  <span data-toggle="tooltip"
                        title={ToolTips.SHUFFLE_READ_BLOCKED_TIME} data-placement="right">
                    <input type="checkbox" name={TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME}/>
                    <span class="additional-metric-title">Shuffle读阻塞时长</span>
                  </span>
                </li>
                <li>
                  <span data-toggle="tooltip"
                        title={ToolTips.SHUFFLE_READ_REMOTE_SIZE} data-placement="right">
                    <input type="checkbox" name={TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE}/>
                    <span class="additional-metric-title">Shuffle远程读取</span>
                  </span>
                </li>
              }}
              <li>
                <span data-toggle="tooltip"
                      title={ToolTips.RESULT_SERIALIZATION_TIME} data-placement="right">
                  <input type="checkbox" name={TaskDetailsClassNames.RESULT_SERIALIZATION_TIME}/>
                  <span class="additional-metric-title">结果序列化时长</span>
                </span>
              </li>
              <li>
                <span data-toggle="tooltip"
                      title={ToolTips.GETTING_RESULT_TIME} data-placement="right">
                  <input type="checkbox" name={TaskDetailsClassNames.GETTING_RESULT_TIME}/>
                  <span class="additional-metric-title">结果获取时长</span>
                </span>
              </li>
              {if (displayPeakExecutionMemory) {
                <li>
                  <span data-toggle="tooltip"
                        title={ToolTips.PEAK_EXECUTION_MEMORY} data-placement="right">
                    <input type="checkbox" name={TaskDetailsClassNames.PEAK_EXECUTION_MEMORY}/>
                    <span class="additional-metric-title">内存峰值</span>
                  </span>
                </li>
              }}
            </ul>
          </div>
        </div>

      val dagViz = UIUtils.showDagVizForStage(
        stageId, operationGraphListener.getOperationGraphForStage(stageId))

      val accumulableHeaders: Seq[String] = Seq("Accumulable", "Value")
      def accumulableRow(acc: AccumulableInfo): Seq[Node] = {
        (acc.name, acc.value) match {
          case (Some(name), Some(value)) => <tr><td>{name}</td><td>{value}</td></tr>
          case _ => Seq.empty[Node]
        }
      }
      val accumulableTable = UIUtils.listingTable(
        accumulableHeaders,
        accumulableRow,
        externalAccumulables.toSeq)

      val page: Int = {
        // If the user has changed to a larger page size, then go to page 1 in order to avoid
        // IndexOutOfBoundsException.
        if (taskPageSize <= taskPrevPageSize) {
          taskPage
        } else {
          1
        }
      }
      val currentTime = System.currentTimeMillis()
      val (taskTable, taskTableHTML) = try {
        val _taskTable = new TaskPagedTable(
          parent.conf,
          UIUtils.prependBaseUri(parent.basePath) +
            s"/stages/stage?id=${stageId}&attempt=${stageAttemptId}",
          tasks,
          hasAccumulators,
          stageData.hasInput,
          stageData.hasOutput,
          stageData.hasShuffleRead,
          stageData.hasShuffleWrite,
          stageData.hasBytesSpilled,
          currentTime,
          pageSize = taskPageSize,
          sortColumn = taskSortColumn,
          desc = taskSortDesc
        )
        (_taskTable, _taskTable.table(page))
      } catch {
        case e @ (_ : IllegalArgumentException | _ : IndexOutOfBoundsException) =>
          val errorMessage =
            <div class="alert alert-error">
              <p>Error while rendering stage table:</p>
              <pre>
                {Utils.exceptionString(e)}
              </pre>
            </div>
          (null, errorMessage)
      }

      val jsForScrollingDownToTaskTable =
        <script>
          {Unparsed {
            """
              |$(function() {
              |  if (/.*&task.sort=.*$/.test(location.search)) {
              |    var topOffset = $("#tasks-section").offset().top;
              |    $("html,body").animate({scrollTop: topOffset}, 200);
              |  }
              |});
            """.stripMargin
           }
          }
        </script>

      val taskIdsInPage = if (taskTable == null) Set.empty[Long]
        else taskTable.dataSource.slicedTaskIds

      // Excludes tasks which failed and have incomplete metrics
      val validTasks = tasks.filter(t => t.taskInfo.status == "SUCCESS" && t.metrics.isDefined)

      val summaryTable: Option[Seq[Node]] =
        if (validTasks.size == 0) {
          None
        }
        else {
          def getDistributionQuantiles(data: Seq[Double]): IndexedSeq[Double] =
            Distribution(data).get.getQuantiles()
          def getFormattedTimeQuantiles(times: Seq[Double]): Seq[Node] = {
            getDistributionQuantiles(times).map { millis =>
              <td>{UIUtils.formatDuration(millis.toLong)}</td>
            }
          }
          def getFormattedSizeQuantiles(data: Seq[Double]): Seq[Elem] = {
            getDistributionQuantiles(data).map(d => <td>{Utils.bytesToString(d.toLong)}</td>)
          }

          val deserializationTimes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.executorDeserializeTime.toDouble
          }
          val deserializationQuantiles =
            <td>
              <span data-toggle="tooltip" title={ToolTips.TASK_DESERIALIZATION_TIME}
                    data-placement="right">
                任务解序列化时长
              </span>
            </td> +: getFormattedTimeQuantiles(deserializationTimes)

          val serviceTimes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.executorRunTime.toDouble
          }
          val serviceQuantiles = <td>时长</td> +: getFormattedTimeQuantiles(serviceTimes)

          val gcTimes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.jvmGCTime.toDouble
          }
          val gcQuantiles =
            <td>
              <span data-toggle="tooltip"
                  title={ToolTips.GC_TIME} data-placement="right">GC 时长
              </span>
            </td> +: getFormattedTimeQuantiles(gcTimes)

          val serializationTimes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.resultSerializationTime.toDouble
          }
          val serializationQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.RESULT_SERIALIZATION_TIME} data-placement="right">
                结果序列化时长
              </span>
            </td> +: getFormattedTimeQuantiles(serializationTimes)

          val gettingResultTimes = validTasks.map { taskUIData: TaskUIData =>
            getGettingResultTime(taskUIData.taskInfo, currentTime).toDouble
          }
          val gettingResultQuantiles =
            <td>
              <span data-toggle="tooltip"
                  title={ToolTips.GETTING_RESULT_TIME} data-placement="right">
                获取结果时长
              </span>
            </td> +:
            getFormattedTimeQuantiles(gettingResultTimes)

          val peakExecutionMemory = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.peakExecutionMemory.toDouble
          }
          val peakExecutionMemoryQuantiles = {
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.PEAK_EXECUTION_MEMORY} data-placement="right">
                内存峰值
              </span>
            </td> +: getFormattedSizeQuantiles(peakExecutionMemory)
          }

          // The scheduler delay includes the network delay to send the task to the worker
          // machine and to send back the result (but not the time to fetch the task result,
          // if it needed to be fetched from the block manager on the worker).
          val schedulerDelays = validTasks.map { taskUIData: TaskUIData =>
            getSchedulerDelay(taskUIData.taskInfo, taskUIData.metrics.get, currentTime).toDouble
          }
          val schedulerDelayTitle = <td><span data-toggle="tooltip"
            title={ToolTips.SCHEDULER_DELAY} data-placement="right">Scheduler Delay</span></td>
          val schedulerDelayQuantiles = schedulerDelayTitle +:
            getFormattedTimeQuantiles(schedulerDelays)
          def getFormattedSizeQuantilesWithRecords(data: Seq[Double], records: Seq[Double])
            : Seq[Elem] = {
            val recordDist = getDistributionQuantiles(records).iterator
            getDistributionQuantiles(data).map(d =>
              <td>{s"${Utils.bytesToString(d.toLong)} / ${recordDist.next().toLong}"}</td>
            )
          }

          val inputSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.inputMetrics.bytesRead.toDouble
          }

          val inputRecords = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.inputMetrics.recordsRead.toDouble
          }

          val inputQuantiles = <td>Input Size / Records</td> +:
            getFormattedSizeQuantilesWithRecords(inputSizes, inputRecords)

          val outputSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.outputMetrics.bytesWritten.toDouble
          }

          val outputRecords = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.outputMetrics.recordsWritten.toDouble
          }

          val outputQuantiles = <td>Output Size / Records</td> +:
            getFormattedSizeQuantilesWithRecords(outputSizes, outputRecords)

          val shuffleReadBlockedTimes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleReadMetrics.fetchWaitTime.toDouble
          }
          val shuffleReadBlockedQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.SHUFFLE_READ_BLOCKED_TIME} data-placement="right">
                Shuffle 读阻塞时常
              </span>
            </td> +:
            getFormattedTimeQuantiles(shuffleReadBlockedTimes)

          val shuffleReadTotalSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleReadMetrics.totalBytesRead.toDouble
          }
          val shuffleReadTotalRecords = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleReadMetrics.recordsRead.toDouble
          }
          val shuffleReadTotalQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.SHUFFLE_READ} data-placement="right">
                Shuffle 读大小/记录
              </span>
            </td> +:
            getFormattedSizeQuantilesWithRecords(shuffleReadTotalSizes, shuffleReadTotalRecords)

          val shuffleReadRemoteSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleReadMetrics.remoteBytesRead.toDouble
          }
          val shuffleReadRemoteQuantiles =
            <td>
              <span data-toggle="tooltip"
                    title={ToolTips.SHUFFLE_READ_REMOTE_SIZE} data-placement="right">
                Shuffle 远程读取
              </span>
            </td> +:
            getFormattedSizeQuantiles(shuffleReadRemoteSizes)

          val shuffleWriteSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleWriteMetrics.bytesWritten.toDouble
          }

          val shuffleWriteRecords = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.shuffleWriteMetrics.recordsWritten.toDouble
          }

          val shuffleWriteQuantiles = <td>Shuffle Write Size / Records</td> +:
            getFormattedSizeQuantilesWithRecords(shuffleWriteSizes, shuffleWriteRecords)

          val memoryBytesSpilledSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.memoryBytesSpilled.toDouble
          }
          val memoryBytesSpilledQuantiles = <td>Shuffle spill (memory)</td> +:
            getFormattedSizeQuantiles(memoryBytesSpilledSizes)

          val diskBytesSpilledSizes = validTasks.map { taskUIData: TaskUIData =>
            taskUIData.metrics.get.diskBytesSpilled.toDouble
          }
          val diskBytesSpilledQuantiles = <td>Shuffle spill (disk)</td> +:
            getFormattedSizeQuantiles(diskBytesSpilledSizes)

          val listings: Seq[Seq[Node]] = Seq(
            <tr>{serviceQuantiles}</tr>,
            <tr class={TaskDetailsClassNames.SCHEDULER_DELAY}>{schedulerDelayQuantiles}</tr>,
            <tr class={TaskDetailsClassNames.TASK_DESERIALIZATION_TIME}>
              {deserializationQuantiles}
            </tr>
            <tr>{gcQuantiles}</tr>,
            <tr class={TaskDetailsClassNames.RESULT_SERIALIZATION_TIME}>
              {serializationQuantiles}
            </tr>,
            <tr class={TaskDetailsClassNames.GETTING_RESULT_TIME}>{gettingResultQuantiles}</tr>,
            if (displayPeakExecutionMemory) {
              <tr class={TaskDetailsClassNames.PEAK_EXECUTION_MEMORY}>
                {peakExecutionMemoryQuantiles}
              </tr>
            } else {
              Nil
            },
            if (stageData.hasInput) <tr>{inputQuantiles}</tr> else Nil,
            if (stageData.hasOutput) <tr>{outputQuantiles}</tr> else Nil,
            if (stageData.hasShuffleRead) {
              <tr class={TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME}>
                {shuffleReadBlockedQuantiles}
              </tr>
              <tr>{shuffleReadTotalQuantiles}</tr>
              <tr class={TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE}>
                {shuffleReadRemoteQuantiles}
              </tr>
            } else {
              Nil
            },
            if (stageData.hasShuffleWrite) <tr>{shuffleWriteQuantiles}</tr> else Nil,
            if (stageData.hasBytesSpilled) <tr>{memoryBytesSpilledQuantiles}</tr> else Nil,
            if (stageData.hasBytesSpilled) <tr>{diskBytesSpilledQuantiles}</tr> else Nil)

          val quantileHeaders = Seq("监控", "最小值", "25百分位",
            "中值", "75百分位", "最大值")
          // The summary table does not use CSS to stripe rows, which doesn't work with hidden
          // rows (instead, JavaScript in table.js is used to stripe the non-hidden rows).
          Some(UIUtils.listingTable(
            quantileHeaders,
            identity[Seq[Node]],
            listings,
            fixedWidth = true,
            id = Some("task-summary-table"),
            stripeRowsWithCss = false))
        }

      val executorTable = new ExecutorTable(stageId, stageAttemptId, parent)

      val maybeAccumulableTable: Seq[Node] =
        if (hasAccumulators) { <h4>Accumulators</h4> ++ accumulableTable } else Seq()

      val content =
        summary ++
        dagViz ++
        showAdditionalMetrics ++
        makeTimeline(
          // Only show the tasks in the table
          stageData.taskData.values.toSeq.filter(t => taskIdsInPage.contains(t.taskInfo.taskId)),
          currentTime) ++
        <h4> {numCompleted} 已完成任务的监控摘要</h4> ++
        <div>{summaryTable.getOrElse("No tasks have reported metrics yet.")}</div> ++
        <h4>执行器监控汇总</h4> ++ executorTable.toNodeSeq ++
        maybeAccumulableTable ++
        <h4 id="tasks-section">任务 ({totalTasksNumStr})</h4> ++
          taskTableHTML ++ jsForScrollingDownToTaskTable
      UIUtils.headerSparkPage(stageHeader, content, parent, showVisualization = true)
    }
  }

  def makeTimeline(tasks: Seq[TaskUIData], currentTime: Long): Seq[Node] = {
    val executorsSet = new HashSet[(String, String)]
    var minLaunchTime = Long.MaxValue
    var maxFinishTime = Long.MinValue

    val executorsArrayStr =
      tasks.sortBy(-_.taskInfo.launchTime).take(MAX_TIMELINE_TASKS).map { taskUIData =>
        val taskInfo = taskUIData.taskInfo
        val executorId = taskInfo.executorId
        val host = taskInfo.host
        executorsSet += ((executorId, host))

        val launchTime = taskInfo.launchTime
        val finishTime = if (!taskInfo.running) taskInfo.finishTime else currentTime
        val totalExecutionTime = finishTime - launchTime
        minLaunchTime = launchTime.min(minLaunchTime)
        maxFinishTime = finishTime.max(maxFinishTime)

        def toProportion(time: Long) = time.toDouble / totalExecutionTime * 100

        val metricsOpt = taskUIData.metrics
        val shuffleReadTime =
          metricsOpt.map(_.shuffleReadMetrics.fetchWaitTime).getOrElse(0L)
        val shuffleReadTimeProportion = toProportion(shuffleReadTime)
        val shuffleWriteTime =
          (metricsOpt.map(_.shuffleWriteMetrics.writeTime).getOrElse(0L) / 1e6).toLong
        val shuffleWriteTimeProportion = toProportion(shuffleWriteTime)

        val serializationTime = metricsOpt.map(_.resultSerializationTime).getOrElse(0L)
        val serializationTimeProportion = toProportion(serializationTime)
        val deserializationTime = metricsOpt.map(_.executorDeserializeTime).getOrElse(0L)
        val deserializationTimeProportion = toProportion(deserializationTime)
        val gettingResultTime = getGettingResultTime(taskUIData.taskInfo, currentTime)
        val gettingResultTimeProportion = toProportion(gettingResultTime)
        val schedulerDelay =
          metricsOpt.map(getSchedulerDelay(taskInfo, _, currentTime)).getOrElse(0L)
        val schedulerDelayProportion = toProportion(schedulerDelay)

        val executorOverhead = serializationTime + deserializationTime
        val executorRunTime = if (taskInfo.running) {
          totalExecutionTime - executorOverhead - gettingResultTime
        } else {
          metricsOpt.map(_.executorRunTime).getOrElse(
            totalExecutionTime - executorOverhead - gettingResultTime)
        }
        val executorComputingTime = executorRunTime - shuffleReadTime - shuffleWriteTime
        val executorComputingTimeProportion =
          math.max(100 - schedulerDelayProportion - shuffleReadTimeProportion -
            shuffleWriteTimeProportion - serializationTimeProportion -
            deserializationTimeProportion - gettingResultTimeProportion, 0)

        val schedulerDelayProportionPos = 0
        val deserializationTimeProportionPos =
          schedulerDelayProportionPos + schedulerDelayProportion
        val shuffleReadTimeProportionPos =
          deserializationTimeProportionPos + deserializationTimeProportion
        val executorRuntimeProportionPos =
          shuffleReadTimeProportionPos + shuffleReadTimeProportion
        val shuffleWriteTimeProportionPos =
          executorRuntimeProportionPos + executorComputingTimeProportion
        val serializationTimeProportionPos =
          shuffleWriteTimeProportionPos + shuffleWriteTimeProportion
        val gettingResultTimeProportionPos =
          serializationTimeProportionPos + serializationTimeProportion

        val index = taskInfo.index
        val attempt = taskInfo.attemptNumber

        val svgTag =
          if (totalExecutionTime == 0) {
            // SPARK-8705: Avoid invalid attribute error in JavaScript if execution time is 0
            """<svg class="task-assignment-timeline-duration-bar"></svg>"""
          } else {
           s"""<svg class="task-assignment-timeline-duration-bar">
                 |<rect class="scheduler-delay-proportion"
                   |x="$schedulerDelayProportionPos%" y="0px" height="26px"
                   |width="$schedulerDelayProportion%"></rect>
                 |<rect class="deserialization-time-proportion"
                   |x="$deserializationTimeProportionPos%" y="0px" height="26px"
                   |width="$deserializationTimeProportion%"></rect>
                 |<rect class="shuffle-read-time-proportion"
                   |x="$shuffleReadTimeProportionPos%" y="0px" height="26px"
                   |width="$shuffleReadTimeProportion%"></rect>
                 |<rect class="executor-runtime-proportion"
                   |x="$executorRuntimeProportionPos%" y="0px" height="26px"
                   |width="$executorComputingTimeProportion%"></rect>
                 |<rect class="shuffle-write-time-proportion"
                   |x="$shuffleWriteTimeProportionPos%" y="0px" height="26px"
                   |width="$shuffleWriteTimeProportion%"></rect>
                 |<rect class="serialization-time-proportion"
                   |x="$serializationTimeProportionPos%" y="0px" height="26px"
                   |width="$serializationTimeProportion%"></rect>
                 |<rect class="getting-result-time-proportion"
                   |x="$gettingResultTimeProportionPos%" y="0px" height="26px"
                   |width="$gettingResultTimeProportion%"></rect></svg>""".stripMargin
          }
        val timelineObject =
          s"""
             |{
               |'className': 'task task-assignment-timeline-object',
               |'group': '$executorId',
               |'content': '<div class="task-assignment-timeline-content"
                 |data-toggle="tooltip" data-placement="top"
                 |data-html="true" data-container="body"
                 |data-title="${s"Task " + index + " (attempt " + attempt + ")"}<br>
                 |Status: ${taskInfo.status}<br>
                 |Launch Time: ${UIUtils.formatDate(new Date(launchTime))}
                 |${
                     if (!taskInfo.running) {
                       s"""<br>Finish Time: ${UIUtils.formatDate(new Date(finishTime))}"""
                     } else {
                        ""
                      }
                   }
                 |<br>Scheduler Delay: $schedulerDelay ms
                 |<br>Task Deserialization Time: ${UIUtils.formatDuration(deserializationTime)}
                 |<br>Shuffle Read Time: ${UIUtils.formatDuration(shuffleReadTime)}
                 |<br>Executor Computing Time: ${UIUtils.formatDuration(executorComputingTime)}
                 |<br>Shuffle Write Time: ${UIUtils.formatDuration(shuffleWriteTime)}
                 |<br>Result Serialization Time: ${UIUtils.formatDuration(serializationTime)}
                 |<br>Getting Result Time: ${UIUtils.formatDuration(gettingResultTime)}">
                 |$svgTag',
               |'start': new Date($launchTime),
               |'end': new Date($finishTime)
             |}
           |""".stripMargin.replaceAll("""[\r\n]+""", " ")
        timelineObject
      }.mkString("[", ",", "]")

    val groupArrayStr = executorsSet.map {
      case (executorId, host) =>
        s"""
            {
              'id': '$executorId',
              'content': '$executorId / $host',
            }
          """
    }.mkString("[", ",", "]")

    <span class="expand-task-assignment-timeline">
      <span class="expand-task-assignment-timeline-arrow arrow-closed"></span>
      <a>事件时间表</a>
    </span> ++
    <div id="task-assignment-timeline" class="collapsed">
      {
        if (MAX_TIMELINE_TASKS < tasks.size) {
          <strong>
            This stage has more than the maximum number of tasks that can be shown in the
            visualization! Only the most recent {MAX_TIMELINE_TASKS} tasks
            (of {tasks.size} total) are shown.
          </strong>
        } else {
          Seq.empty
        }
      }
      <div class="control-panel">
        <div id="task-assignment-timeline-zoom-lock">
          <input type="checkbox"></input>
          <span>开启 zooming</span>
        </div>
      </div>
      {TIMELINE_LEGEND}
    </div> ++
    <script type="text/javascript">
      {Unparsed(s"drawTaskAssignmentTimeline(" +
      s"$groupArrayStr, $executorsArrayStr, $minLaunchTime, $maxFinishTime, " +
        s"${UIUtils.getTimeZoneOffset()})")}
    </script>
  }

}

private[ui] object StagePage {
  private[ui] def getGettingResultTime(info: TaskInfo, currentTime: Long): Long = {
    if (info.gettingResult) {
      if (info.finished) {
        info.finishTime - info.gettingResultTime
      } else {
        // The task is still fetching the result.
        currentTime - info.gettingResultTime
      }
    } else {
      0L
    }
  }

  private[ui] def getSchedulerDelay(
      info: TaskInfo, metrics: TaskMetricsUIData, currentTime: Long): Long = {
    if (info.finished) {
      val totalExecutionTime = info.finishTime - info.launchTime
      val executorOverhead = (metrics.executorDeserializeTime +
        metrics.resultSerializationTime)
      math.max(
        0,
        totalExecutionTime - metrics.executorRunTime - executorOverhead -
          getGettingResultTime(info, currentTime))
    } else {
      // The task is still running and the metrics like executorRunTime are not available.
      0L
    }
  }
}

private[ui] case class TaskTableRowInputData(inputSortable: Long, inputReadable: String)

private[ui] case class TaskTableRowOutputData(outputSortable: Long, outputReadable: String)

private[ui] case class TaskTableRowShuffleReadData(
    shuffleReadBlockedTimeSortable: Long,
    shuffleReadBlockedTimeReadable: String,
    shuffleReadSortable: Long,
    shuffleReadReadable: String,
    shuffleReadRemoteSortable: Long,
    shuffleReadRemoteReadable: String)

private[ui] case class TaskTableRowShuffleWriteData(
    writeTimeSortable: Long,
    writeTimeReadable: String,
    shuffleWriteSortable: Long,
    shuffleWriteReadable: String)

private[ui] case class TaskTableRowBytesSpilledData(
    memoryBytesSpilledSortable: Long,
    memoryBytesSpilledReadable: String,
    diskBytesSpilledSortable: Long,
    diskBytesSpilledReadable: String)

/**
 * Contains all data that needs for sorting and generating HTML. Using this one rather than
 * TaskUIData to avoid creating duplicate contents during sorting the data.
 */
private[ui] class TaskTableRowData(
    val index: Int,
    val taskId: Long,
    val attempt: Int,
    val speculative: Boolean,
    val status: String,
    val taskLocality: String,
    val executorIdAndHost: String,
    val launchTime: Long,
    val duration: Long,
    val formatDuration: String,
    val schedulerDelay: Long,
    val taskDeserializationTime: Long,
    val gcTime: Long,
    val serializationTime: Long,
    val gettingResultTime: Long,
    val peakExecutionMemoryUsed: Long,
    val accumulators: Option[String], // HTML
    val input: Option[TaskTableRowInputData],
    val output: Option[TaskTableRowOutputData],
    val shuffleRead: Option[TaskTableRowShuffleReadData],
    val shuffleWrite: Option[TaskTableRowShuffleWriteData],
    val bytesSpilled: Option[TaskTableRowBytesSpilledData],
    val error: String)

private[ui] class TaskDataSource(
    tasks: Seq[TaskUIData],
    hasAccumulators: Boolean,
    hasInput: Boolean,
    hasOutput: Boolean,
    hasShuffleRead: Boolean,
    hasShuffleWrite: Boolean,
    hasBytesSpilled: Boolean,
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedDataSource[TaskTableRowData](pageSize) {
  import StagePage._

  // Convert TaskUIData to TaskTableRowData which contains the final contents to show in the table
  // so that we can avoid creating duplicate contents during sorting the data
  private val data = tasks.map(taskRow).sorted(ordering(sortColumn, desc))

  private var _slicedTaskIds: Set[Long] = null

  override def dataSize: Int = data.size

  override def sliceData(from: Int, to: Int): Seq[TaskTableRowData] = {
    val r = data.slice(from, to)
    _slicedTaskIds = r.map(_.taskId).toSet
    r
  }

  def slicedTaskIds: Set[Long] = _slicedTaskIds

  private def taskRow(taskData: TaskUIData): TaskTableRowData = {
    val info = taskData.taskInfo
    val metrics = taskData.metrics
    val duration = if (info.status == "RUNNING") info.timeRunning(currentTime)
      else metrics.map(_.executorRunTime).getOrElse(1L)
    val formatDuration = if (info.status == "RUNNING") UIUtils.formatDuration(duration)
      else metrics.map(m => UIUtils.formatDuration(m.executorRunTime)).getOrElse("")
    val schedulerDelay = metrics.map(getSchedulerDelay(info, _, currentTime)).getOrElse(0L)
    val gcTime = metrics.map(_.jvmGCTime).getOrElse(0L)
    val taskDeserializationTime = metrics.map(_.executorDeserializeTime).getOrElse(0L)
    val serializationTime = metrics.map(_.resultSerializationTime).getOrElse(0L)
    val gettingResultTime = getGettingResultTime(info, currentTime)

    val externalAccumulableReadable = info.accumulables
      .filterNot(_.internal)
      .flatMap { a =>
        (a.name, a.update) match {
          case (Some(name), Some(update)) => Some(StringEscapeUtils.escapeHtml4(s"$name: $update"))
          case _ => None
        }
      }
    val peakExecutionMemoryUsed = metrics.map(_.peakExecutionMemory).getOrElse(0L)

    val maybeInput = metrics.map(_.inputMetrics)
    val inputSortable = maybeInput.map(_.bytesRead).getOrElse(0L)
    val inputReadable = maybeInput
      .map(m => s"${Utils.bytesToString(m.bytesRead)}")
      .getOrElse("")
    val inputRecords = maybeInput.map(_.recordsRead.toString).getOrElse("")

    val maybeOutput = metrics.map(_.outputMetrics)
    val outputSortable = maybeOutput.map(_.bytesWritten).getOrElse(0L)
    val outputReadable = maybeOutput
      .map(m => s"${Utils.bytesToString(m.bytesWritten)}")
      .getOrElse("")
    val outputRecords = maybeOutput.map(_.recordsWritten.toString).getOrElse("")

    val maybeShuffleRead = metrics.map(_.shuffleReadMetrics)
    val shuffleReadBlockedTimeSortable = maybeShuffleRead.map(_.fetchWaitTime).getOrElse(0L)
    val shuffleReadBlockedTimeReadable =
      maybeShuffleRead.map(ms => UIUtils.formatDuration(ms.fetchWaitTime)).getOrElse("")

    val totalShuffleBytes = maybeShuffleRead.map(_.totalBytesRead)
    val shuffleReadSortable = totalShuffleBytes.getOrElse(0L)
    val shuffleReadReadable = totalShuffleBytes.map(Utils.bytesToString).getOrElse("")
    val shuffleReadRecords = maybeShuffleRead.map(_.recordsRead.toString).getOrElse("")

    val remoteShuffleBytes = maybeShuffleRead.map(_.remoteBytesRead)
    val shuffleReadRemoteSortable = remoteShuffleBytes.getOrElse(0L)
    val shuffleReadRemoteReadable = remoteShuffleBytes.map(Utils.bytesToString).getOrElse("")

    val maybeShuffleWrite = metrics.map(_.shuffleWriteMetrics)
    val shuffleWriteSortable = maybeShuffleWrite.map(_.bytesWritten).getOrElse(0L)
    val shuffleWriteReadable = maybeShuffleWrite
      .map(m => s"${Utils.bytesToString(m.bytesWritten)}").getOrElse("")
    val shuffleWriteRecords = maybeShuffleWrite
      .map(_.recordsWritten.toString).getOrElse("")

    val maybeWriteTime = metrics.map(_.shuffleWriteMetrics.writeTime)
    val writeTimeSortable = maybeWriteTime.getOrElse(0L)
    val writeTimeReadable = maybeWriteTime.map(t => t / (1000 * 1000)).map { ms =>
      if (ms == 0) "" else UIUtils.formatDuration(ms)
    }.getOrElse("")

    val maybeMemoryBytesSpilled = metrics.map(_.memoryBytesSpilled)
    val memoryBytesSpilledSortable = maybeMemoryBytesSpilled.getOrElse(0L)
    val memoryBytesSpilledReadable =
      maybeMemoryBytesSpilled.map(Utils.bytesToString).getOrElse("")

    val maybeDiskBytesSpilled = metrics.map(_.diskBytesSpilled)
    val diskBytesSpilledSortable = maybeDiskBytesSpilled.getOrElse(0L)
    val diskBytesSpilledReadable = maybeDiskBytesSpilled.map(Utils.bytesToString).getOrElse("")

    val input =
      if (hasInput) {
        Some(TaskTableRowInputData(inputSortable, s"$inputReadable / $inputRecords"))
      } else {
        None
      }

    val output =
      if (hasOutput) {
        Some(TaskTableRowOutputData(outputSortable, s"$outputReadable / $outputRecords"))
      } else {
        None
      }

    val shuffleRead =
      if (hasShuffleRead) {
        Some(TaskTableRowShuffleReadData(
          shuffleReadBlockedTimeSortable,
          shuffleReadBlockedTimeReadable,
          shuffleReadSortable,
          s"$shuffleReadReadable / $shuffleReadRecords",
          shuffleReadRemoteSortable,
          shuffleReadRemoteReadable
        ))
      } else {
        None
      }

    val shuffleWrite =
      if (hasShuffleWrite) {
        Some(TaskTableRowShuffleWriteData(
          writeTimeSortable,
          writeTimeReadable,
          shuffleWriteSortable,
          s"$shuffleWriteReadable / $shuffleWriteRecords"
        ))
      } else {
        None
      }

    val bytesSpilled =
      if (hasBytesSpilled) {
        Some(TaskTableRowBytesSpilledData(
          memoryBytesSpilledSortable,
          memoryBytesSpilledReadable,
          diskBytesSpilledSortable,
          diskBytesSpilledReadable
        ))
      } else {
        None
      }

    new TaskTableRowData(
      info.index,
      info.taskId,
      info.attemptNumber,
      info.speculative,
      info.status,
      info.taskLocality.toString,
      s"${info.executorId} / ${info.host}",
      info.launchTime,
      duration,
      formatDuration,
      schedulerDelay,
      taskDeserializationTime,
      gcTime,
      serializationTime,
      gettingResultTime,
      peakExecutionMemoryUsed,
      if (hasAccumulators) Some(externalAccumulableReadable.mkString("<br/>")) else None,
      input,
      output,
      shuffleRead,
      shuffleWrite,
      bytesSpilled,
      taskData.errorMessage.getOrElse(""))
  }

  /**
   * Return Ordering according to sortColumn and desc
   */
  private def ordering(sortColumn: String, desc: Boolean): Ordering[TaskTableRowData] = {
    val ordering = sortColumn match {
      case "序号" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.Int.compare(x.index, y.index)
      }
      case "ID" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.Long.compare(x.taskId, y.taskId)
      }
      case "尝试" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.Int.compare(x.attempt, y.attempt)
      }
      case "状态" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.String.compare(x.status, y.status)
      }
      case "本地级别" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.String.compare(x.taskLocality, y.taskLocality)
      }
      case "执行器ID/节点" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.String.compare(x.executorIdAndHost, y.executorIdAndHost)
      }
      case "启动时间" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.Long.compare(x.launchTime, y.launchTime)
      }
      case "时长" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.Long.compare(x.duration, y.duration)
      }
      case "调度延迟" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.Long.compare(x.schedulerDelay, y.schedulerDelay)
      }
      case "任务解序列化时长" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.Long.compare(x.taskDeserializationTime, y.taskDeserializationTime)
      }
      case "GC时长" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.Long.compare(x.gcTime, y.gcTime)
      }
      case "结果序列化时长" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.Long.compare(x.serializationTime, y.serializationTime)
      }
      case "获取结果时长" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.Long.compare(x.gettingResultTime, y.gettingResultTime)
      }
      case "内存峰值" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.Long.compare(x.peakExecutionMemoryUsed, y.peakExecutionMemoryUsed)
      }
      case "累加器" =>
        if (hasAccumulators) {
          new Ordering[TaskTableRowData] {
            override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
              Ordering.String.compare(x.accumulators.get, y.accumulators.get)
          }
        } else {
          throw new IllegalArgumentException(
            "无法按照该项排序")
        }
      case "输入大小/记录数" =>
        if (hasInput) {
          new Ordering[TaskTableRowData] {
            override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
              Ordering.Long.compare(x.input.get.inputSortable, y.input.get.inputSortable)
          }
        } else {
          throw new IllegalArgumentException(
            "无法按照该项排序")
        }
      case "输出大小/记录数" =>
        if (hasOutput) {
          new Ordering[TaskTableRowData] {
            override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
              Ordering.Long.compare(x.output.get.outputSortable, y.output.get.outputSortable)
          }
        } else {
          throw new IllegalArgumentException(
            "无法按照该项排序")
        }
      // ShuffleRead
      case "Shuffle 读阻塞时长" =>
        if (hasShuffleRead) {
          new Ordering[TaskTableRowData] {
            override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
              Ordering.Long.compare(x.shuffleRead.get.shuffleReadBlockedTimeSortable,
                y.shuffleRead.get.shuffleReadBlockedTimeSortable)
          }
        } else {
          throw new IllegalArgumentException(
            "无法按照该项排序Shuffle Read Blocked Time because of no shuffle reads")
        }
      case "Shuffle 读大小/记录数" =>
        if (hasShuffleRead) {
          new Ordering[TaskTableRowData] {
            override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
              Ordering.Long.compare(x.shuffleRead.get.shuffleReadSortable,
                y.shuffleRead.get.shuffleReadSortable)
          }
        } else {
          throw new IllegalArgumentException(
            "无法按照该项排序")
        }
      case "Shuffle 远程读取" =>
        if (hasShuffleRead) {
          new Ordering[TaskTableRowData] {
            override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
              Ordering.Long.compare(x.shuffleRead.get.shuffleReadRemoteSortable,
                y.shuffleRead.get.shuffleReadRemoteSortable)
          }
        } else {
          throw new IllegalArgumentException(
            "无法按照该项排序")
        }
      // ShuffleWrite
      case "写时长" =>
        if (hasShuffleWrite) {
          new Ordering[TaskTableRowData] {
            override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
              Ordering.Long.compare(x.shuffleWrite.get.writeTimeSortable,
                y.shuffleWrite.get.writeTimeSortable)
          }
        } else {
          throw new IllegalArgumentException(
            "无法按照该项排序")
        }
      case "Shuffle写大小/记录数" =>
        if (hasShuffleWrite) {
          new Ordering[TaskTableRowData] {
            override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
              Ordering.Long.compare(x.shuffleWrite.get.shuffleWriteSortable,
                y.shuffleWrite.get.shuffleWriteSortable)
          }
        } else {
          throw new IllegalArgumentException(
            "无法按照该项排序")
        }
      // BytesSpilled
      case "Shuffle溢出（内存）" =>
        if (hasBytesSpilled) {
          new Ordering[TaskTableRowData] {
            override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
              Ordering.Long.compare(x.bytesSpilled.get.memoryBytesSpilledSortable,
                y.bytesSpilled.get.memoryBytesSpilledSortable)
          }
        } else {
          throw new IllegalArgumentException(
            "无法按照该项排序")
        }
      case "Shuffle溢出（磁盘）" =>
        if (hasBytesSpilled) {
          new Ordering[TaskTableRowData] {
            override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
              Ordering.Long.compare(x.bytesSpilled.get.diskBytesSpilledSortable,
                y.bytesSpilled.get.diskBytesSpilledSortable)
          }
        } else {
          throw new IllegalArgumentException(
            "无法按照该项排序")
        }
      case "Errors" => new Ordering[TaskTableRowData] {
        override def compare(x: TaskTableRowData, y: TaskTableRowData): Int =
          Ordering.String.compare(x.error, y.error)
      }
      case unknownColumn => throw new IllegalArgumentException(s"未知列: $unknownColumn")
    }
    if (desc) {
      ordering.reverse
    } else {
      ordering
    }
  }

}

private[ui] class TaskPagedTable(
    conf: SparkConf,
    basePath: String,
    data: Seq[TaskUIData],
    hasAccumulators: Boolean,
    hasInput: Boolean,
    hasOutput: Boolean,
    hasShuffleRead: Boolean,
    hasShuffleWrite: Boolean,
    hasBytesSpilled: Boolean,
    currentTime: Long,
    pageSize: Int,
    sortColumn: String,
    desc: Boolean) extends PagedTable[TaskTableRowData] {

  // We only track peak memory used for unsafe operators
  private val displayPeakExecutionMemory = conf.getBoolean("spark.sql.unsafe.enabled", true)

  override def tableId: String = "task-table"

  override def tableCssClass: String =
    "table table-bordered table-condensed table-striped table-head-clickable"

  override def pageSizeFormField: String = "task.pageSize"

  override def prevPageSizeFormField: String = "task.prevPageSize"

  override def pageNumberFormField: String = "task.page"

  override val dataSource: TaskDataSource = new TaskDataSource(
    data,
    hasAccumulators,
    hasInput,
    hasOutput,
    hasShuffleRead,
    hasShuffleWrite,
    hasBytesSpilled,
    currentTime,
    pageSize,
    sortColumn,
    desc)

  override def pageLink(page: Int): String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    basePath +
      s"&$pageNumberFormField=$page" +
      s"&task.sort=$encodedSortColumn" +
      s"&task.desc=$desc" +
      s"&$pageSizeFormField=$pageSize"
  }

  override def goButtonFormPath: String = {
    val encodedSortColumn = URLEncoder.encode(sortColumn, "UTF-8")
    s"$basePath&task.sort=$encodedSortColumn&task.desc=$desc"
  }

  def headers: Seq[Node] = {
    val taskHeadersAndCssClasses: Seq[(String, String)] =
      Seq(
        ("序号", ""), ("ID", ""), ("尝试", ""), ("状态", ""), ("本地级别", ""),
        ("执行器ID/节点", ""), ("启动时间", ""), ("时长", ""),
        ("调度延迟", TaskDetailsClassNames.SCHEDULER_DELAY),
        ("任务解序列化时长", TaskDetailsClassNames.TASK_DESERIALIZATION_TIME),
        ("GC时长", ""),
        ("结果序列化时长", TaskDetailsClassNames.RESULT_SERIALIZATION_TIME),
        ("获取结果时长", TaskDetailsClassNames.GETTING_RESULT_TIME)) ++
        {
          if (displayPeakExecutionMemory) {
            Seq(("内存峰值", TaskDetailsClassNames.PEAK_EXECUTION_MEMORY))
          } else {
            Nil
          }
        } ++
        {if (hasAccumulators) Seq(("累加器", "")) else Nil} ++
        {if (hasInput) Seq(("输入大小/记录数", "")) else Nil} ++
        {if (hasOutput) Seq(("输出大小/记录数", "")) else Nil} ++
        {if (hasShuffleRead) {
          Seq(("Shuffle 读阻塞时长", TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME),
            ("Shuffle 读大小/记录数", ""),
            ("Shuffle 远程读取", TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE))
        } else {
          Nil
        }} ++
        {if (hasShuffleWrite) {
          Seq(("写时长", ""), ("Shuffle写大小/记录数", ""))
        } else {
          Nil
        }} ++
        {if (hasBytesSpilled) {
          Seq(("Shuffle溢出（内存）", ""), ("Shuffle溢出（磁盘）", ""))
        } else {
          Nil
        }} ++
        Seq(("Errors", ""))

    if (!taskHeadersAndCssClasses.map(_._1).contains(sortColumn)) {
      throw new IllegalArgumentException(s"未知列: $sortColumn")
    }

    val headerRow: Seq[Node] = {
      taskHeadersAndCssClasses.map { case (header, cssClass) =>
        if (header == sortColumn) {
          val headerLink = Unparsed(
            basePath +
              s"&task.sort=${URLEncoder.encode(header, "UTF-8")}" +
              s"&task.desc=${!desc}" +
              s"&task.pageSize=$pageSize")
          val arrow = if (desc) "&#x25BE;" else "&#x25B4;" // UP or DOWN
          <th class={cssClass}>
            <a href={headerLink}>
              {header}
              <span>&nbsp;{Unparsed(arrow)}</span>
            </a>
          </th>
        } else {
          val headerLink = Unparsed(
            basePath +
              s"&task.sort=${URLEncoder.encode(header, "UTF-8")}" +
              s"&task.pageSize=$pageSize")
          <th class={cssClass}>
            <a href={headerLink}>
              {header}
            </a>
          </th>
        }
      }
    }
    <thead>{headerRow}</thead>
  }

  def row(task: TaskTableRowData): Seq[Node] = {
    <tr>
      <td>{task.index}</td>
      <td>{task.taskId}</td>
      <td>{if (task.speculative) s"${task.attempt} (speculative)" else task.attempt.toString}</td>
      <td>{task.status}</td>
      <td>{task.taskLocality}</td>
      <td>{task.executorIdAndHost}</td>
      <td>{UIUtils.formatDate(new Date(task.launchTime))}</td>
      <td>{task.formatDuration}</td>
      <td class={TaskDetailsClassNames.SCHEDULER_DELAY}>
        {UIUtils.formatDuration(task.schedulerDelay)}
      </td>
      <td class={TaskDetailsClassNames.TASK_DESERIALIZATION_TIME}>
        {UIUtils.formatDuration(task.taskDeserializationTime)}
      </td>
      <td>
        {if (task.gcTime > 0) UIUtils.formatDuration(task.gcTime) else ""}
      </td>
      <td class={TaskDetailsClassNames.RESULT_SERIALIZATION_TIME}>
        {UIUtils.formatDuration(task.serializationTime)}
      </td>
      <td class={TaskDetailsClassNames.GETTING_RESULT_TIME}>
        {UIUtils.formatDuration(task.gettingResultTime)}
      </td>
      {if (displayPeakExecutionMemory) {
        <td class={TaskDetailsClassNames.PEAK_EXECUTION_MEMORY}>
          {Utils.bytesToString(task.peakExecutionMemoryUsed)}
        </td>
      }}
      {if (task.accumulators.nonEmpty) {
        <td>{Unparsed(task.accumulators.get)}</td>
      }}
      {if (task.input.nonEmpty) {
        <td>{task.input.get.inputReadable}</td>
      }}
      {if (task.output.nonEmpty) {
        <td>{task.output.get.outputReadable}</td>
      }}
      {if (task.shuffleRead.nonEmpty) {
        <td class={TaskDetailsClassNames.SHUFFLE_READ_BLOCKED_TIME}>
          {task.shuffleRead.get.shuffleReadBlockedTimeReadable}
        </td>
        <td>{task.shuffleRead.get.shuffleReadReadable}</td>
        <td class={TaskDetailsClassNames.SHUFFLE_READ_REMOTE_SIZE}>
          {task.shuffleRead.get.shuffleReadRemoteReadable}
        </td>
      }}
      {if (task.shuffleWrite.nonEmpty) {
        <td>{task.shuffleWrite.get.writeTimeReadable}</td>
        <td>{task.shuffleWrite.get.shuffleWriteReadable}</td>
      }}
      {if (task.bytesSpilled.nonEmpty) {
        <td>{task.bytesSpilled.get.memoryBytesSpilledReadable}</td>
        <td>{task.bytesSpilled.get.diskBytesSpilledReadable}</td>
      }}
      {errorMessageCell(task.error)}
    </tr>
  }

  private def errorMessageCell(error: String): Seq[Node] = {
    val isMultiline = error.indexOf('\n') >= 0
    // Display the first line by default
    val errorSummary = StringEscapeUtils.escapeHtml4(
      if (isMultiline) {
        error.substring(0, error.indexOf('\n'))
      } else {
        error
      })
    val details = if (isMultiline) {
      // scalastyle:off
      <span onclick="this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')"
            class="expand-details">
        +details
      </span> ++
        <div class="stacktrace-details collapsed">
          <pre>{error}</pre>
        </div>
      // scalastyle:on
    } else {
      ""
    }
    <td>{errorSummary}{details}</td>
  }
}
