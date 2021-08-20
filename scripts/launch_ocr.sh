apt -y update
apt -y install git
apt -y install screen
git clone https://github.com/Envinorma/data-tasks.git
cd data-tasks
apt -y install python3-pip
pip3 install virtualenv
virtualenv venv
source venv/bin/activate
apt-get -y remove ocrmypdf
apt-get -y update
apt-get -y install \
    ghostscript \
    icc-profiles-free \
    liblept5 \
    libxml2 \
    pngquant \
    python3-pip \
    tesseract-ocr \
    tesseract-ocr-fra \
    zlib1g
pip3 install -r requirements.txt
pip3 install ipython==7.19.0
cp default_config.ini config.ini
X='OS_AUTH_URL="https://auth.cloud.ovh.net/v3/" OS_IDENTITY_API_VERSION=3 OS_USER_DOMAIN_NAME=Default OS_PROJECT_DOMAIN_NAME=Default OS_TENANT_ID=XXXXXXXX OS_TENANT_NAME=XXXXXXXX OS_USERNAME=XXXXXXXX OS_PASSWORD=XXXXXXXX OS_REGION_NAME=SBG python3 -m tasks.ocr_ap.ocr_ap'
screen -d -m bash -c "$X" -S ocr
