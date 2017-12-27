# MultiOutputFormat
自定义格式的ORCfile的多路输出

MapReduce程序的Outputformat;
分为两个版本,实现功能类似；

**v0.1**
一个文件
MultipleOutputFormat.java

**功能**

先定义好orcStruct，直接写入不同文件

**使用方法**

见UserRecomTagMR2.java

**v0.2:**

三个文件
- DefineOrcStruct.java
- MultipleOutputs.java
- OrcStructMRWriter.java

**功能：**

输入ORC文件的格式，输出到不同文件中。

**使用方法：**

类似于UserRecomTagMR2.java
write方法传入四个参数；第一个是实例化的OrcStruct对象，第二个是原始的colStruct,
第三个是自定义的colStruct，第四个是文件名
