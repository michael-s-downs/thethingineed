### This code is property of the GGAO ###


# SET BASE IMAGE
ARG python_version
FROM python:${python_version}-slim-buster

# NVIDIA SUPPORT
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES=compute,utility
ENV NVIDIA_REQUIRE_CUDA "cuda>=10.1"

# COPY PACKAGES
COPY ./pypackages /packages

# INSTALL DEPENDENCIES
RUN apt-get update && apt-get --no-install-recommends install -y \
    build-essential git ghostscript imagemagick \
    libmagic1 libpq-dev \
    libsm6 libxext6 libxrender-dev \
    ffmpeg \
	libtesseract-dev tesseract-ocr \
    wget tesseract-ocr-spa \
    && rm -rf /var/lib/apt/lists/*

# ADD REQUIREMENTS AND POLICY TO IMAGEMAGICK
ADD requirements.txt /opt/uhis_docker/
ADD policy.xml /etc/ImageMagick-6/policy.xml

# MOVE WORKER DIR AND INSTALL AND DELETE LIBRARIES
WORKDIR /opt/uhis_docker
RUN pip install -r requirements.txt
RUN pip install genai-sdk-services --no-index --find-links=/packages
RUN rm -rf /packages

# CLEAN IMAGE
RUN apt-get -y remove build-essential && \
    apt-get clean && \
    apt -y autoremove && \
    rm -rf /var/lib/apt/lists/*
