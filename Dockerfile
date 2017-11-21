FROM ubuntu/ubuntu:latest

LABEL maintainer Ilija Vukotic <ivukotic@cern.ch>

#################
#### curl/wget
#################
RUN apt-get update && apt-get install curl wget -y

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt-get update && apt-get install -y --allow-unauthenticated \
        build-essential \
        git \
        libzmq3-dev \
        module-init-tools \
        pkg-config \
        python \
        python-dev \
        python3 \
        rsync \
        software-properties-common \
        unzip \
        zip \
        zlib1g-dev \
        vim \
        python-pip \
        python3-pip \
        && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip && \
    pip3 install --upgrade pip

##############################
# Python 2 packages
##############################

RUN pip2 --no-cache-dir install elasticsearch

#############################
# Python 3 packages
#############################

RUN pip3 --no-cache-dir install elasticsearch

# build info
RUN echo "Timestamp:" `date --utc` | tee /image-build-info.txt