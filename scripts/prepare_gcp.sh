# Script for installing docker and pulling this repo prior to executing the main docker container
apt-get update
apt-get install \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release

curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo \
  "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null

apt-get update
apt-get install docker-ce docker-ce-cli containerd.io
apt-get install git
git clone https://github.com/Envinorma/data-tasks
git checkout add-containerization-and-new-tasks

# For local execution
# Copy config.ini
# docker run -it --rm -v ~/Envinorma/data-tasks:/mnt -e PYTHONPATH='/mnt' -e STORAGE_SEED_FOLDER='/data/seed' -e STORAGE_AM_REPOSITORY_FOLDER='/data/repo' envinorma-data-tasks bash
# cd /mnt;python -m tasks.flows

# remote execution
# docker run -it -e PYTHONPATH='/usr/src/app' -e ... envinorma-data-tasks

