#FROM python:3.9.0

 
# Set the working directory to /usr/src/app.
#WORKDIR /JobsScraper/data_extraction
 
# Copy the file from the local host to the filesystem of the container at the working directory.
#COPY requirements.txt ./
 
# Install Scrapy specified in requirements.txt.
##RUN pip3 install --no-cache-dir -r requirements.txt
 
# Copy the project source code from the local host to the filesystem of the container at the working directory.
#COPY . .
 
# Run the crawler when the container launches.
#CMD [ "python3", "./start_crawling.py" ]


FROM scrapinghub/scrapinghub-stack-scrapy:2.1
RUN apt-get update
RUN apt-get upgrade -y
RUN apt-get install zip unzip

RUN apt-get install -y wget xvfb unzip
# Set up the Chrome PPA -> (not sure if needed)
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list

# Update the package list
RUN apt-get update -y

# Set up Chromedriver Environment variables and install chrome
ENV CHROMEDRIVER_VERSION 114.0.5735.90
ENV CHROME_VERSION 114.0.5735.90-1
RUN wget --no-verbose -O /tmp/chrome.deb https://dl.google.com/linux/chrome/deb/pool/main/g/google-chrome-stable/google-chrome-stable_${CHROME_VERSION}_amd64.deb \
  && apt install -y /tmp/chrome.deb \
  && rm /tmp/chrome.deb

ENV CHROMEDRIVER_DIR /chromedriver
RUN mkdir $CHROMEDRIVER_DIR

# Download and install Chromedriver
RUN wget -q --continue -P $CHROMEDRIVER_DIR "http://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip"
RUN unzip $CHROMEDRIVER_DIR/chromedriver* -d $CHROMEDRIVER_DIR

# Put Chromedriver into the PATH
ENV PATH $CHROMEDRIVER_DIR:$PATH

#ENV TERM xterm
#ENV SCRAPY_SETTINGS_MODULE AiJobs.settings
WORKDIR /data_extraction
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python3", "./start_crawling.py" ]