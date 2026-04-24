#!/bin/sh

CONTAINER_NAME=jt808
IMAGE_NAME=jt808-server
DEPLOY_DIR=/home/ftpuser/jtt808-deploy

# 停止并删除旧容器
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo "停止容器 ${CONTAINER_NAME}..."
    docker stop ${CONTAINER_NAME}
    echo "删除容器 ${CONTAINER_NAME}..."
    docker rm ${CONTAINER_NAME}
fi

# 删除所有同名镜像（不管tag）
OLD_IMAGES=$(docker images --format '{{.Repository}}:{{.Tag}}' | grep "^${IMAGE_NAME}:")
if [ -n "${OLD_IMAGES}" ]; then
    echo "删除旧镜像..."
    echo "${OLD_IMAGES}" | xargs docker rmi
fi

# 重新构建镜像
echo "构建镜像..."
cd ${DEPLOY_DIR}
docker build -t ${IMAGE_NAME} .

# 创建必要目录
mkdir -p ${DEPLOY_DIR}/jt_data/media_file
mkdir -p ${DEPLOY_DIR}/jt_data/alarm_file
mkdir -p ${DEPLOY_DIR}/logs
chmod 777 ${DEPLOY_DIR}/jt_data/media_file
chmod 777 ${DEPLOY_DIR}/jt_data/alarm_file
chmod 777 ${DEPLOY_DIR}/logs

# 运行容器
echo "启动容器..."
docker run -d \
  --name ${CONTAINER_NAME} \
  --restart=always \
  -p 8100:8100 \
  -p 7100:7100 \
  -p 7100:7100/udp \
  -p 7200:7200 \
  -v ${DEPLOY_DIR}/jt_data/media_file:/jt_data/media_file \
  -v ${DEPLOY_DIR}/jt_data/alarm_file:/jt_data/alarm_file \
  -v ${DEPLOY_DIR}/logs:/app/logs \
  ${IMAGE_NAME}

echo "启动成功，查看日志："
docker logs -f ${CONTAINER_NAME}
