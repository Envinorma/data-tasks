FROM python:3

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./am_diffs/compute_am_diffs.py" ]
# CMD [ "python", "./backup_bo_database.py" ]
# CMD [ "python", "./data_build/load_ams_in_ovh.py" ]
