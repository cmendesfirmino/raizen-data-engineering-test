FROM apache/airflow:2.2.3
USER root


# Install OpenJDK-11, Libreoffice and vim
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         libreoffice \
         openjdk-11-jdk \
         ant \ 
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

USER airflow
RUN cd /opt/airflow \
   && mkdir data data/raw data/clean data/stage 

RUN pip install --no-cache-dir lxml xlrd