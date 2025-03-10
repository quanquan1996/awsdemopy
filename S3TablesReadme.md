# 环境配置
1. 控制台创建表存储桶
2. 打开启用"与AWS分析服务集成"
3. 创建IAM角色,给到S3 Tables,Lake Formation,Glue相关权限
# 部署分析服务
## 部署duckdb lambda
步骤：
- 创建ECR镜像仓库
- 构建lambda镜像,参考代码：https://github.com/quanquan1996/aws-s3tables-duckdb-lambda-image.git
- docker buildx build --platform linux/amd64 --provenance=false -t docker-image:test .
- 验证Aws ecr aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 111122223333.dkr.ecr.us-east-1.amazonaws.com
- 创建ECE存储库 aws ecr create-repository --repository-name hello-world --region us-east-1 --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
- 运行tag命令docker tag docker-image:test <ECRrepositoryUri>:latest
- push镜像docker push 111122223333.dkr.ecr.us-east-1.amazonaws.com/hello-world:latest
- 在控制台选择push的镜像部署lambda。参考教程 https://docs.aws.amazon.com/lambda/latest/dg/python-image.html
## 部署Athena Spark Lambda
- 创建Athena Spark工作组，分配角色
- 复制git仓库下的AthenaSpark.py部署lambda
## 部署EMR Serverless Lambda
- 创建EMR Serverless application
- 复制S3TablesEMRServerlessAsyncSubmit.py部署lambda
- 复制S3TablesEMRServerlessAsyncCheckAndGet.py部署lambda
## 部署测试页面
- 使用Api Gateway 创建api,代理上述部署的lambda
- 修改本仓库的htmldemo的html文件把api地址替换成api gateway的api地址,并把html文件上传到s3
- 使用CloudFront创建静态网站
- 访问CloudFront的域名查看网站效果
# 权限控制
登录Lake Formation赋予角色相关catalog权限
# 创建表
- 使用Athena Spark Lambda或者Emr Serverless Lambda或者pyspark.py示例代码执行sql   
- 创建命名空间： CREATE NAMESPACE IF NOT EXISTS s3tablesbucket.my_namespace或者CREATE NAMESPACE IF NOT EXISTS my_namespace
- 创建表：CREATE TABLE IF NOT EXISTS s3tablesbucket.my_namespace.my_table (id int, name string)
# 权限控制
AWS Lake Formation上配置对于的表和库权限
# 配置数据入湖
- 创建nlb代理mysql
- 创建终端节点服务
- 加入firehose.amazonaws.com作为允许委托人
- 创建firehose流(参考教程 https://aws.amazon.com/jp/blogs/news/replicate-changes-from-databases-to-apache-iceberg-tables-using-amazon-data-firehose/)
- 测试数据流式入湖
