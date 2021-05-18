heroku pg:backups:capture --app envinorma-back-office
heroku pg:backups:download --app envinorma-back-office
now=`date +"%Y-%m-%d-%H-%M"`
mv latest.dump backups/$now.dump