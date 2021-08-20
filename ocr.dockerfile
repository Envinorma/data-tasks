FROM python:3

WORKDIR /usr/src/app


RUN apt-get -y update
RUN apt-get -y remove ocrmypdf
RUN apt-get -y install \
    ghostscript \
    icc-profiles-free \
    liblept5 \
    libxml2 \
    pngquant \
    python3-pip \
    tesseract-ocr \
    tesseract-ocr-fra \
    zlib1g

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir /data
RUN mkdir /data/seed
RUN mkdir /data/repo

COPY . .

CMD [ "python", "-m", "tasks.ocr_ap.ocr_ap" ]

