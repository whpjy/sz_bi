# 使用 PyTorch 的官方 CUDA 镜像作为基础镜像
FROM python:3.10

RUN pip install --upgrade pip

# 复制requirements.txt到容器中
COPY requirements.txt /usr/src/app/requirements.txt

# 安装requirements.txt中的依赖
RUN pip install -r /usr/src/app/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple


