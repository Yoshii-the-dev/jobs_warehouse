
#--------------------

FROM flink:1.17

RUN apt-get update -y && \
apt-get install -y build-essential libssl-dev lzma liblzma-dev zlib1g-dev libbz2-dev libffi-dev libsqlite3-dev && \
wget https://www.python.org/ftp/python/3.9.0/Python-3.9.0.tgz && \
tar -xvf Python-3.9.0.tgz && \
cd Python-3.9.0 && \
./configure --without-tests --enable-shared && \
make -j6 && \
make install && \
ldconfig /usr/local/lib && \
cd .. && rm -f Python-3.9.0.tgz && rm -rf Python-3.9.0 && \
ln -s /usr/local/bin/python3 /usr/local/bin/python && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip --no-cache-dir apache-flink-libraries==1.17.0 && pip3 install --no-cache-dir apache-flink==1.17.0

COPY ner_textcat_model_pack /ner_textcat_model_pack
RUN pip3 install --no-cache-dir /ner_textcat_model_pack/en_pipeline-0.0.0/dist/en_pipeline-0.0.0.tar.gz

COPY requirements.txt /opt/flink/requirements.txt
RUN pip3 install --no-cache-dir -r /opt/flink/requirements.txt && \
    python3 -m spacy download en_core_web_sm && \
    python3 -c "import nltk; nltk.download(['punkt', 'averaged_perceptron_tagger', 'maxent_ne_chunker', 'treebank', 'maxent_treebank_pos_tagger', 'words'], download_dir='/usr/local/nltk_data')" 


RUN chmod -R 777 /usr/local/lib/python3.9/site-packages/locationtagger
    #chmod 664 /usr/local/lib/python3.9/site-packages/locationtagger/locationdata.db

RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.0-1.17/flink-connector-jdbc-3.1.0-1.17.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-postgres-cdc/2.4.0/flink-sql-connector-postgres-cdc-2.4.0.jar; \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-python/1.17.0/flink-python-1.17.0.jar; \
    wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.6.0.jar;
#   wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mongodb-cdc/2.4.0/flink-sql-connector-mongodb-cdc-2.4.0.jar;
COPY descriptions_first_processing_job_dev.py /opt/flink/descriptions_first_processing_job_dev.py

COPY udfs.py /opt/flink/udfs.py

