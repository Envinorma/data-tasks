git clone https://github.com/Envinorma/data-tasks
cd data-tasks
# Copy config.ini
docker build -t envinorma-data-tasks .
docker run -it --rm -e PYTHONPATH='/usr/src/app' -e STORAGE_SEED_FOLDER='/data/seed' -e STORAGE_AM_REPOSITORY_FOLDER='/data/repo' envinorma-data-tasks


# remote execution
# docker run -it -e PYTHONPATH='/usr/src/app' -e ... envinorma-data-tasks

# Mac local execution
# docker run -it --rm -v ~/Envinorma/data-tasks:/mnt -e PYTHONPATH='/mnt' -e STORAGE_SEED_FOLDER='/data/seed' -e STORAGE_AM_REPOSITORY_FOLDER='/data/repo' envinorma-data-tasks bash

