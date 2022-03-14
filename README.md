# flinkcdc-to-mysql

###### pom中依赖和框架下面的lib中的重复解决方案

```
在pom中对应的依赖包后面添加<scope>provided</scope>
```

###### 

###### maven打包添加依赖

```
在pom中添加相应的配置
```



###### flink程序部署linux 

```
on yarn-session 
启动yarn-session 然后提交任务
./bin/yarn-session.sh -n 4 -jm 1024 -tm 4096
/data/program/flink-1.13.3/bin/flink run --class MysqlToMysqlMain  ./flinkcdc-to-mysql-1.0-SNAPSHOT-jar-with-dependencies.jar
kill 命令
yarn application -kill application_1644544835838_28942
```

