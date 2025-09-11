# dcm2niix를 실행할 수 있는 Docker 환경
FROM ubuntu:22.04

# 비대화형 모드로 설정 (패키지 설치 시 프롬프트 방지)
ENV DEBIAN_FRONTEND=noninteractive

# 시스템 업데이트 및 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    unzip \
    build-essential \
    cmake \
    git \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# dcm2niix 최신 버전 다운로드 및 컴파일
WORKDIR /tmp
RUN git clone https://github.com/rordenlab/dcm2niix.git && \
    cd dcm2niix && \
    mkdir build && \
    cd build && \
    cmake -DZLIB_IMPLEMENTATION=Cloudflare -DUSE_JPEGLS=ON -DUSE_OPENJPEG=ON .. && \
    make && \
    make install

# 작업 디렉토리 설정
WORKDIR /data

# dcm2niix가 PATH에 있는지 확인
RUN dcm2niix -h

# 기본 명령어
CMD ["/bin/bash"]