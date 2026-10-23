#!/bin/bash
# Restarts the back-end bot (the `bot` entry of the Procfile), from the image built by build.sh.
# The web service is restarted separately, with webservice.sh.
toolforge jobs delete rustbot
rm ~/rustbot.*
toolforge jobs run --mem 2000Mi --cpu 1 --continuous --mount=all \
	--image tool-wdrc/tool-wdrc:latest \
	--command bot \
	--filelog -o /data/project/wdrc/rustbot.out -e /data/project/wdrc/rustbot.err \
	rustbot
