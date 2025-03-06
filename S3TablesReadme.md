# 环境配置
1. 控制台创建表存储桶
2. 打开启用"与AWS分析服务集成“
# 部署分析服务
## 部署duckdb lambda
步骤：
- 创建ECR镜像仓库
- 构建lambda镜像上传,参考代码：https://github.com/quanquan1996/aws-s3tables-duckdb-lambda-image.git
- 部署lambda
## 部署Athena Spark Lambda
- 创建Athena Spark工作组
- 复制AthenaSpark.py部署lambda
## 部署EMR Serverless Lambda
- 创建EMR Serverless application
- 复制S3TablesEMRServerlessAsyncSubmit.py部署lambda
- 复制S3TablesEMRServerlessAsyncCheckAndGet.py部署lambda
# 创建表
使用Athena Spark Lambda或者Emr Serverless Lambda或者pyspark.py示例代码执行sql   
创建命名空间： CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.my_namespace或者CREATE NAMESPACE IF NOT EXISTS my_namespace
创建表：CREATE TABLE IF NOT EXISTS s3tablesbucket.my_namespace.my_table (id int, name string)
# 权限控制
AWS Lake Formation上配置对于的表和库权限
# 配置数据入湖
- 创建nlb代理mysql
- 创建终端节点服务
- 加入firehose.amazonaws.com作为允许委托人
- 创建firehose流