#Spark 汉化文档
####下载源码进行编译测试

初次编译
```bash
./build/mvn -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.3 -Phive -Phive-thriftserver -DskipTests clean package
```

二次变异
```bash
./build/mvn -Pyarn -Phadoop-2.7 -Dhadoop.version=2.7.3 -Phive -Phive-thriftserver -DskipTests package
```

#### 源码分析及修改
所有的页面都是Scala动态生成的：
涉及到的大部分界面相关文件位于/usr/soft/spark-2.0.1/core/src/main/scala/org/apache/spark/ui下
与HistoryServer相关页面位于/usr/soft/spark-2.0.1/core/src/main/scala/org/apache/spark/deploy/history下
具体修改记录见Github:https://github.com/yantaiv/Spark201ChineseTrans
注意源代码中tabs名称同时做了url的文件指向，所以重新修改了SparkUI.scala 和 WebUI.scala的SparkUITab方法和WebUITab方法（添加了三个入参的构造方法，用来接收中文名），首次修改中没有注意到该问题，启动时会报改页面多次重定向的报错。


####替换HDP文件及所遇到的问题

##### Spark yarn启动时报错
编译完成后，在/usr/soft/spark-2.0.1/assembly/target/scala-2.11/jars下有完整的打包结果
取出其中的spark-core_2.11-2.0.1.jar 和 spark-yarn_2.11-2.0.1.jar 替换HDP中/usr/hdp/2.5.0.0-1245/spark2/jars对应jar包

此时spark-shell运行没问题，但通过Yarn提交任务时报错
![Alt text](./1476778387951.png)
查看关于jersey的jar包

```bash
find ./ |grep jersey
```
发现yarn的lib包下面使用的是1.9的jar，而spark下使用的是2.22.2的jar包，

解决方案:是将/usr/hdp/2.5.0.0-1245/hadoop/lib/ 下的jersey-core-1.9.jar 和 jersey-client-1.9.jar 这两个包拷贝到$SPARK_HOME/jars目录下（网上说还要将该目录下原本的 jersey-client-2.22.2.jar改名，但我没有）,yarn模式的spark就可以正常启动

参考页面：
https://my.oschina.net/xiaozhublog/blog/737902
https://markobigdata.com/2016/08/01/apache-spark-2-0-0-installation-and-configuration/

##### Spark yarn提交时报错

```
Exception message:
/mnt/hdfs01/hadoop/yarn/local/usercache/test/appcache/application_1427875242006_0029/container_1427875242006_0029_02_000001/launch_container.sh: line 27: $PWD:$PWD/__spark__.jar:$HADOOP_CONF_DIR:/usr/hdp/current/hadoop-client/*:/usr/hdp/current/hadoop-client/lib/*:/usr/hdp/current/hadoop-hdfs-client/*:/usr/hdp/current/hadoop-hdfs-client/lib/*:/usr/hdp/current/hadoop-yarn-client/*:/usr/hdp/current/hadoop-yarn-client/lib/*:$PWD/mr-framework/hadoop/share/hadoop/mapreduce/*:$PWD/mr-framework/hadoop/share/hadoop/mapreduce/lib/*:$PWD/mr-framework/hadoop/share/hadoop/common/*:$PWD/mr-framework/hadoop/share/hadoop/common/lib/*:$PWD/mr-framework/hadoop/share/hadoop/yarn/*:$PWD/mr-framework/hadoop/share/hadoop/yarn/lib/*:$PWD/mr-framework/hadoop/share/hadoop/hdfs/*:$PWD/mr-framework/hadoop/share/hadoop/hdfs/lib/*:/usr/hdp/${hdp.version}/hadoop/lib/hadoop-lzo-0.6.0.${hdp.version}.jar:/etc/hadoop/conf/secure: bad substitution
```

解决方案，在平台中的Spark配置（自定义 spark2-defaults）添加如下配置
![Alt text](./1476779057980.png)

spark.driver.extraJavaOptions -Dhdp.version=2.5.0.0–1245
spark.yarn.am.extraJavaOptions -Dhdp.version=2.2.0.0–1245

参考页面：
http://stackoverflow.com/questions/29470542/spark-1-3-0-running-pi-example-on-yarn-fails
