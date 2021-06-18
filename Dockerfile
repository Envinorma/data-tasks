FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir /data
RUN mkdir /data/seed
RUN mkdir /data/repo

RUN curl https://cli-assets.heroku.com/install-ubuntu.sh | sh

COPY . .

CMD [ "python", "-m", "tasks.flows" ]

