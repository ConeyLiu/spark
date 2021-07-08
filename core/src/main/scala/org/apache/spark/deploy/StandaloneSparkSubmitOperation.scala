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

package org.apache.spark.deploy

import org.apache.spark.SparkConf
import org.apache.spark.api.r.RUtils
import org.apache.spark.deploy.rest.RestSubmissionClient
import org.apache.spark.internal.config.SUBMIT_PYTHON_FILES
import org.apache.spark.util.DependencyUtils.mergeFileLists


private[spark] class StandaloneSparkSubmitOperation(
    master: String) extends CommonSparkSubmitOperation(master) {


  override def checkSparkSubmitArguments(args: SparkSubmitArguments): Unit = {
    super.checkSparkSubmitArguments(args)
    if (args.deployMode == "cluster" && args.isPython) {
      error("Cluster deploy mode is currently not supported for python " +
        "applications on standalone clusters.")
    }

    if (args.deployMode == "cluster" && args.isR) {
      error("Cluster deploy mode is currently not supported for R " +
        "applications on standalone clusters.")
    }
  }

  override def prepareSubmitEnvironments(
      args: SparkSubmitArguments, conf: SparkConf): SparkSubmitEnvironments = {
    args.toSparkConf(Some(conf))
    val hadoopConf = SparkHadoopUtil.newConfiguration(conf)
    val (localPrimaryResource, localJars, localPyFiles) =
      resolveAndDownLoadRemoteFiles(args, conf, hadoopConf)
    resolveMainClassForJar(args, localPrimaryResource, hadoopConf)
    resolveMainClassForPython(args, localPrimaryResource, localPyFiles)

    // Non-PySpark applications can need Python dependencies.
    if (args.deployMode == "client") {
      args.files = mergeFileLists(args.files, args.pyFiles)
    }

    if (localPyFiles != null) {
      conf.set(SUBMIT_PYTHON_FILES, localPyFiles.split(",").toSeq)
    }

    // TODO: Support distributing R packages with standalone cluster
    if (args.isR && !RUtils.rPackages.isEmpty) {
      error("Distributing R packages with standalone cluster is not supported.")
    }



  }

  override def kill(submissionId: String, conf: SparkConf): Unit = {
    new RestSubmissionClient(master).killSubmission(submissionId)
  }

  override def printSubmissionStatus(submissionId: String, conf: SparkConf): Unit = {
    new RestSubmissionClient(master).requestSubmissionStatus(submissionId)
  }

  override def supports(master: String): Boolean = {
    master.startsWith("spark")
  }
}
