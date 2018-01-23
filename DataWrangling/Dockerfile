FROM ubuntu:latest

USER root

# Install dependencies
RUN apt-get update && apt-get install -y \
    python-pip --upgrade python-pip

RUN pip install --upgrade pip

# install py3
RUN apt-get update -qq \
 && apt-get install --no-install-recommends -y \
    # install python 3
    python3 \
    python3-dev \
    python3-pip \
    python3-setuptools \
    pkg-config \
	vim &&\
	echo "luigi ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers && \
    apt-get clean && \
	rm -rf /var/lib/apt/lists/*

RUN pip3 install --upgrade pip

# install additional python packages
RUN pip3 install ipython
RUN pip install jupyter
RUN pip3 install numpy
RUN pip3 install pandas
RUN pip3 install scikit-learn
RUN pip3 install matplotlib
RUN pip3 install missingno
RUN pip3 install scipy
RUN pip3 install seaborn
#RUN pip install nltk
RUN pip3 install boto3
RUN pip3 install requests
RUN pip3 install plotly

RUN pip3 install luigi

RUN pip3 install jupyter

# configure console
RUN echo 'alias ll="ls --color=auto -lA"' >> /root/.bashrc \
 && echo '"\e[5~": history-search-backward' >> /root/.inputrc \
 && echo '"\e[6~": history-search-forward' >> /root/.inputrc
# default password: keras
ENV PASSWD='sha1:98b767162d34:8da1bc3c75a0f29145769edc977375a373407824'

# dump package lists
RUN dpkg-query -l > /dpkg-query-l.txt \
 && pip2 freeze > /pip2-freeze.txt \
 && pip3 freeze > /pip3-freeze.txt

# for jupyter
EXPOSE 8888

WORKDIR /src/

RUN mkdir /src/assignment1
RUN mkdir /src/assignment1/output
RUN mkdir /src/assignment1/logs
ENV OUTPUTPATH=/src/assignment1/output
ENV LOGPATH=/src/assignment1/logs
ENV CONFIGPATH=/src/assignment1
ENV WRANGLEPATH=/src/assignment1
ENV PYTHONPATH=$WRANGLEPATH:$PYTHONPATH

ADD run.sh /src/assignment1/
ADD wrangle.py /src/assignment1/
ADD configWrangle.json /src/assignment1/
ADD MA_CleanDataEDA.ipynb /src/assignment1/
ADD CleanDataStaticEDA.csv /src/assignment1/


#testing
#ADD MA_21062017_WBAN_14702.csv /src/assignment1/output/
#RUN chmod 777 /src/assignment1/output/MA_21062017_WBAN_14702.csv

RUN chmod 777 /src/assignment1/run.sh
RUN chmod 777 /src/assignment1/wrangle.py
RUN chmod 777 /src/assignment1/configWrangle.json
RUN chmod 777 /src/assignment1/MA_CleanDataEDA.ipynb
RUN chmod 777 /src/assignment1/CleanDataStaticEDA.csv

CMD /src/assignment1/run.sh

#RUN mkdir /srv/nb1/

#CMD /bin/bash -c 'jupyter notebook --no-browser --allow-root --ip=* --NotebookApp.password="$PASSWD" "$@"'

#CMD /bin/bash -c 'jupyter notebook --no-browser --ip=* --NotebookApp.password="keras" "$@"'