---
sidebar_position: 1
authors:
- name: teaho
  title: teaho2015@gmail.com
  url: https://github.com/teaho2015
  tags: [mq]
---

# RocketMQ源码结构与运行

## 简介

RocketMQ孕育于阿里巴巴，现在算是国内业务功能消息队列的首选了。
这首先因为它服务于很多厂的大流量线上，同时它提供如普通消息、延迟消息、顺序消息、死信队列等一系列功能，它号称是金融级别的业务消息队列首选。

RocketMQ 4架构简单，功能丰富，且拓展性良好。RocketMQ 5加入了Proxy、等一系列组件，协议也换成了GRPC，但也向下兼容。
下面我们来一一分析。

## 架构

RocketMQ 5.0的架构整理如下：

![](img/rocketmq-arch.jpg)

## 源码结构及运行

打开[rocketmq](https://github.com/apache/rocketmq)源码，我们可以看到如下目录，我做了些注释做简单讲解，省略了部分我认为不重要目录。
````
rocketmq
├── acl acl权限控制，根据accessKey、accessSecret、sign去做访问控制。
├── bazel 
├── broker broker。
├── client remoting协议的client sdk。
├── common 工具类库模块。
├── container 
├── controller 支持自动切换主从的模块，内嵌在name srv里。
├── distribution 打包发布的模块。
├── docs 文档。
├── example 例子。
├── filter 
├── namesrv namesrv。
├── openmessaging openmessaging支持。
├── proxy 代理设置。
├── remoting remoting协议。
├── srvutil 也是一个工具类库。
├── store 是broker和tieredstore的底层存储依赖，做数据存储。
├── tieredstore
├── tools 
````

### broker启动


### namesrv启动


## 
