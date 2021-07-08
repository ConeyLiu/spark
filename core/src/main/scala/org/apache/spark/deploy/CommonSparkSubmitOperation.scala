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


import java.io.File
import java.net.URI
import java.util.jar.JarInputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkSubmit._
import org.apache.spark.util.DependencyUtils.{downloadFile, downloadFileList, resolveGlobPaths}
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer


private[deploy] abstract class CommonSparkSubmitOperation(
    master: String) extends SparkSubmitOperation {

  override def checkSparkSubmitArguments(args: SparkSubmitArguments): Unit = {
    // Set the deploy mode; default is client mode
    val deployMode: Int = args.deployMode match {
      case "client" | null => CLIENT
      case "cluster" => CLUSTER
      case _ =>
        error("Deploy mode must be either client or cluster")
        -1
    }

    if (deployMode == CLUSTER) {
      if (isShell(args.primaryResource)) {
        error("Cluster deploy mode is not applicable to Spark shells.")
      }
      if (isSqlShell(args.mainClass)) {
        error("Cluster deploy mode is not applicable to Spark SQL shell.")
      }
      if (isThriftServer(args.mainClass)) {
        error("Cluster deploy mode is not applicable to Spark Thrift server.")
      }
    }

    // Update args.deployMode if it is null. It will be passed down as a Spark property later.
    (args.deployMode, deployMode) match {
      case (null, CLIENT) => args.deployMode = "client"
      case (null, CLUSTER) => args.deployMode = "cluster"
      case _ =>
    }
  }

  protected def setupKerberos(args: SparkSubmitArguments): Unit = {
    // If client mode, make sure the keytab is just a local path.
    if (args.deployMode == "client" && Utils.isLocalUri(args.keytab)) {
      args.keytab = new URI(args.keytab).getPath()
    }

    if (!Utils.isLocalUri(args.keytab)) {
      require(new File(args.keytab).exists(), s"Keytab file: ${args.keytab} does not exist")
      UserGroupInformation.loginUserFromKeytab(args.principal, args.keytab)
    }
  }

  protected def resolveAndDownLoadRemoteFiles(
      args: SparkSubmitArguments,
      sparkConf: SparkConf,
      hadoopConf: Configuration): (String, String, String) = {
    // Resolve glob path for different resources.
    args.jars = Option(args.jars).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.files = Option(args.files).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.pyFiles = Option(args.pyFiles).map(resolveGlobPaths(_, hadoopConf)).orNull
    args.archives = Option(args.archives).map(resolveGlobPaths(_, hadoopConf)).orNull

    val targetDir = Utils.createTempDir()

    // In client mode, download remote files.
    var localPrimaryResource: String = null
    var localJars: String = null
    var localPyFiles: String = null
    if (args.deployMode == "client") {
      localPrimaryResource = Option(args.primaryResource).map {
        downloadFile(_, targetDir, sparkConf, hadoopConf)
      }.orNull
      localJars = Option(args.jars).map {
        downloadFileList(_, targetDir, sparkConf, hadoopConf)
      }.orNull
      localPyFiles = Option(args.pyFiles).map {
        downloadFileList(_, targetDir, sparkConf, hadoopConf)
      }.orNull
    }
    (localPrimaryResource, localJars, localPyFiles)
  }

  protected def resolveMainClassForJar(
      args: SparkSubmitArguments,
      localPrimaryResource: String,
      hadoopConf: Configuration): Unit = {
    if (args.mainClass == null && !args.isPython && !args.isR) {
      try {
        val uri = new URI(
          Option(localPrimaryResource).getOrElse(args.primaryResource)
        )
        val fs = FileSystem.get(uri, hadoopConf)

        Utils.tryWithResource(new JarInputStream(fs.open(new Path(uri)))) { jar =>
          args.mainClass = jar.getManifest.getMainAttributes.getValue("Main-Class")
        }
      } catch {
        case e: Throwable =>
          error(
            s"Failed to get main class in JAR with error '${e.getMessage}'. " +
              " Please specify one with --class."
          )
      }

      if (args.mainClass == null) {
        // If we still can't figure out the main class at this point, blow up.
        error("No main class set in JAR; please specify one with --class.")
      }
    }
  }

  protected def resolveMainClassForPython(
      args: SparkSubmitArguments,
      localPrimaryResource: String,
      localPyFiles: String): Unit = {
    // If we're running a python app, set the main class to our specific python runner
    if (args.isPython && args.deployMode == "client") {
      if (args.primaryResource == PYSPARK_SHELL) {
        args.mainClass = "org.apache.spark.api.python.PythonGatewayServer"
      } else {
        // If a python file is provided, add it to the child arguments and list of files to deploy.
        // Usage: PythonAppRunner <main python file> <extra python files> [app arguments]
        args.mainClass = "org.apache.spark.deploy.PythonRunner"
        args.childArgs = ArrayBuffer(localPrimaryResource, localPyFiles) ++ args.childArgs
      }
    }
  }



  /** Throw a SparkException with the given error message. */
  protected def error(msg: String): Unit = throw new SparkException(msg)

}
