# storm-od calc

利用Storm来计算城市公交数据的OD矩阵

## 1. 数据库

    a. 采用了mysql数据库的主从复制架构，node1为master，node2为slave，
       仅对od_week中的表eid、rfid和single_car_od进行了同步。
    b. 拓扑od_calc(TopologyRemoteKafka)会只会读取数据库的rfid这一张表，因此选择读取了node2，执行读操作。
    c. 拓扑od-kafka-save(TopologySaveODKafKa)会将kafka集群中的od回写到表single_car_od，因此选择作用在node1上，执行写操作。
    d. 而node1上的/home/liuxu/downloads/kafka/KafkaProducer.class 用来将表eid中的数据写入到kafka集群中的主题od_week,因为数据量
    不大，此处我仍选择读取node1上的od_week这个数据库，执行读操作。
    e. 帐号： mysql: liuxu/password_mysql 和 root/password_root。

##2. 账户密码

    我在node1-node8上创建了用户liuxu/password_liuxu

##3. 集群信息

    a. 我在node1-node8上搭建了storm集群:
        a.1 node1 == nimbus ui
        a.2 node2-node8 == supervisor
        用脚本monitor.sh对他们的状态进行了监控，能自动重启。
    b. 在node1-node5上搭建了zookeeper集群；
        这个集群的容错性比较高，出于简单我这里就没有对它们进行监控，可以在这些节点上运行命令：zkServer.sh status查看状态。
        可以用"zkServer.sh start/restart"来运行zookeeper服务。
    c. 在node1-node4上搭建了kafka集群
        可用/home/liuxu/storm/kafka-manager监控这个集群的运行状态：./run-kafka-manager.sh
        然后访问 node1_IP:9000
    d. 整个Storm集群的状态可以访问node1_IP:8081看到。

##4. 项目打包及发布到storm集群

    a. 整个项目是采用maven来管理依赖的，因此我们可以采用命令：mvn package -Dmaven.test.skip=true
       来将项目代码打包为jar文件。在执行上面代码之前，我们需要对依赖配置文件pom.xml作一些修改。
       可以看到我们有如下的依赖代码，只是他们的scope值不一样，其它都一样。
       因为在storm集群环境已经提供了storm-core的依赖包，因此我们不必将其和我们代码一起打包，所以
       在执行上面打包命令前，需要注释掉II部分的代码。

```
I.  <dependency>
        <groupId>org.apache.storm</groupId>
        <artifactId>storm-core</artifactId>
        <version>0.9.3</version>
        <scope>provided</scope>
    </dependency>

    <!--when package or install, comment on below dependency-->
II. <dependency>
        <groupId>org.apache.storm</groupId>
        <artifactId>storm-core</artifactId>
        <version>0.9.3</version>
        <scope>compile</scope>
    </dependency>

```
    b. 远程发布包到storm集群
       storm的cookbook中教我们执行topology的方式一般是登录到nimbus节点，采用命令行的方式提交：
       $storm jar xxx.jar qualified-name.class  self-defined-topology-name

       但是本项目可以采用直接在IDE中远程提交的方式来到以上目的。
       即直接运行TopologyRemoteKafka和TopologySaveKafka这两个类。

       但是运行他们需要依赖storm-core中的一些包，因此在需要II这个依赖包，于是需要取消II的注释。
