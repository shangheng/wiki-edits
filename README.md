# wiki-edits
wiki-edits教程是一个监控wikipedia编辑的flink监控程序，实时计算编辑者的编辑的byte数。它通过wikipedia connector来获取数据源，最终把数据sink到kafka中。


### 编写Flink程序

首先我们创建一个WikipediaAnalysis.java文件，并在main方法中添加代码。其大致步骤分为如下：

```
1.获取环境信息
2.为环境信息添加WikipediaEditsSource源
3.根据事件中的用户名为key来区分数据流
4.设置窗口时间为5s
5.聚合当前窗口中相同用户名的事件，最终返回一个tuple2<user，累加的ByteDiff>
6.把tuple2映射为string
7.sink数据到kafka，topic为wiki-result
8.执行操作
```