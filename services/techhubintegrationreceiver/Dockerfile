### This code is property of the GGAO ###

# PARAMS TO BUILD
ARG BASE_IMAGE
FROM $BASE_IMAGE

# COPY OF MACHINE TO DOCKER
COPY ./uhis_docker /opt/uhis_docker

# CREATE DEFAULT DIRECTORY AND INSTALL REQUIREMENTS
WORKDIR /opt/uhis_docker
RUN pip install -r requirements.txt

# CLEAN
RUN apt-get -y remove build-essential && \
    apt-get clean && \
    apt -y autoremove && \
    rm -rf /var/lib/apt/lists/*

# EXPOSE PORT
EXPOSE 8888

# RUN SCRIPT
CMD ["sh", "-c", "gunicorn --bind 0.0.0.0:8888 --workers $GUNICORN_WORKERS --threads $GUNICORN_THREADS --timeout $GUNICORN_TIMEOUT main:app"]