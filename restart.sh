#!/bin/bash
toolforge jobs delete rustbot
touch ~/rustbot.out
mv ~/rustbot.out ~/rustbot_out.bak
toolforge jobs run --mem 2000Mi --cpu 1 --continuous --mount=all \
	--image tool-wdrc/tool-wdrc:latest \
	--command "sh -c 'target/release/wdrc_rs bot /data/project/wdrc/wdrc_rs/config.json'" \
	--filelog -o /data/project/wdrc/rustbot.out -e /data/project/wdrc/rustbot.err \
	rustbot
