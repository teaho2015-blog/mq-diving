---
sidebar_position: 2
authors:
- name: teaho
  title: teaho2015@gmail.com
  url: https://github.com/teaho2015
  tags: [mq]
---

# RocketMQ启动原理

## 简介


## namesrv启动分析

直接上源码：

```` Java title="NamesrvStartup.java"
        try {
            parseCommandlineAndConfigFile(args);
            NamesrvController controller = createAndStartNamesrvController();
            return controller;
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(-1);
        }
````



## broker启动分析




## 


寻址、心跳维持、元数据加载等
