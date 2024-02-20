#!/bin/bash

set -a #Automatically export variables.
source /home/nickgs/rewards-collector/.env
set +a #Disable automatic export.

/home/nickgs/.nvm/versions/node/v18.16.0/bin/node /home/nickgs/rewards-collector/backfiller.js
