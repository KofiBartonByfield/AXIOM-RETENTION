#!/bin/bash
git pull
sudo pkill -f gunicorn
sudo python3 -m gunicorn --certfile=/etc/letsencrypt/live/axiomretention.com/fullchain.pem --keyfile=/etc/letsencrypt/live/axiomretention.com/privkey.pem --workers 3 --bind 0.0.0.0:443 app:app -D
echo "Deployment successful."