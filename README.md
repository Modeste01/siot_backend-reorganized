# siot_backend-reorganized
This repo mainly serves as an interface to conform to the recommended repo layout required by the instructor.

Getting started

Name
siot-backend : This includes the code and docker files for running the Sports IOT LLC database, scraper, and api components of the backend. Currently it also has a boilerplate for the websocket client on the Arduino side, but that is preliminary and will probably eventually be moved to its own repository.

Description
In order to provide updates to our internet-connected sports memorabilia devices in an automated and timely manner, we need a system to track, store, and provide these updates to the devices. This repository is currently divides into three components for the backend and one component for the devices:

Backend

Scraper : This continuously gets updates from the stats.ncaa.org website, parses them, and extracts game events.
DB : This database is set up to store all of the information about connected devices, teams, sports, and games.
API : This component is built on top of FastAPI and provides connectivity both through regular REST endpoints and via websockets.


Device

Arduino client : An example set of code for connecting to the API via websocket, registering the device with associated (UUID, team, sports), and then issuing async callbacks to the code using this project.


Installation
Everything is set up to run via Docker. However, during development and debugging, that often is cumbersome. So there is a single top-level docker-compose file that can be used, but I would recommend only bringing up the components that are in a pretty good state with DockerCompose and then run the rest from a terminal (inside of VSCode) for easy debugging and help from AI.


Install Docker Desktop (or however you want to get Docker and DockerCompose installed)


(If running locally install a python environment)

python3 -m venv env
source env/bin/activate
pip install --upgrade pip
pip install -r scraper/requirements.txt
pip install -r api/requirements.txt




Create the virtual docker network used by the services within Docker

./prepare_docker_network.sh




Bring up one (or more) of the services in the composite-docker-compose.yml file

For the database and database admin website

docker-compose -f composite-docker-compose.yml up db pgadmin



For the scraper

docker-compose -f composite-docker-compose.yml up scraper



Everything

docker-compose -f composite-docker-compose.yml up


All the standard rules about using -d to put in daemon mode



If running from the command line

Make sure to have the python environment activated

source env/bin/activate



Run a command to run the individual component

cd scraper
python scraper



NOTE/TODO: I still don't know the right way to bring up the API, or if it is working inside/outside of Docker




Roadmap
I feel like the scraper and the database are in pretty good shape. It seems to capture and store the game status exactly as expected. Here is a list of items by category that could be improved/added:


Scraper

The end of game detection isn't the best. Sometimes a game will end and the status line won't go to "Final" for quite a while. The per-sport scraper plug-ins could do a better job of detecting game end. For example, in Women's Soccer we know that they don't have stoppage time. So if the time ever gets to "2nd H 90:00" and one of the teams is ahead, we could interpret that as a win.



Database

Maybe add in the datetime when the game went final



API (co-primary work of the 2025-2026 Capstone)

Tons of work needed to verify what is working and what isn't. Not going to make an exhaustive list.
As part of this, the websocket portion needs improved/validated considerably both on the device side (Arduino) and the API side (FastAPI+websockets)



Dashboard (primary work of the 2025-2026 Capstone)

Create a dashboard for monitoring the system
Show connected devices
Show teams being tracked, games for the day, status of the games, etc.
Give a health monitor of the scraper software (detect crashes or exceptions in the scraper system as a whole, detect exceptions in individual scrapers). May require another table in the database.




License
This code is copyright SportsIOT LLC 2025.
