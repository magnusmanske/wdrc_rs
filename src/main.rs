mod api;
mod change;
mod output;
mod recent_changes;
mod revision_compare;
mod telemetry;
mod wdrc;
mod web;
mod wikidata_api;

use anyhow::{anyhow, Result};
use std::env;
use wdrc::*;

const USAGE: &str = "Usage: wdrc_rs <bot|run|server> [config.json]";

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let command = args.get(1).ok_or_else(|| anyhow!(USAGE))?;
    let config_file = args.get(2).map(|s| s.as_str()).unwrap_or("config.json");

    match command.as_str() {
        "bot" => {
            // To stderr, so that telemetry's status messages are not lost.
            wikimisc::logging::init();
            telemetry::start_bot();
            let mut wdrc = WdRc::new(config_file)?;
            let sleep_duration = wdrc.bot_sleep();
            loop {
                match wdrc.run_once().await {
                    // More work waiting: start the next batch immediately.
                    Ok(RunResult::MoreWork) => {
                        telemetry::record_run("more_work");
                        continue;
                    }
                    Ok(RunResult::CaughtUp) => telemetry::record_run("caught_up"),
                    Err(e) => {
                        telemetry::record_run("error");
                        eprintln!("Error: {e}");
                    }
                }
                tokio::time::sleep(sleep_duration).await;
            }
        }
        "run" => {
            WdRc::new(config_file)?.run_once().await?;
            Ok(())
        }
        "server" => {
            wikimisc::logging::init();
            web::run(config_file).await
        }
        other => Err(anyhow!("Unknown command {other:?}. {USAGE}")),
    }
}

/* TESTING
On Toolforge, credentials are read from ~/replica.my.cnf by the `toolforge` crate.
For local testing, open SSH tunnels and add an explicit `url` to the respective
config section, which overrides the ~/replica.my.cnf lookup:
ssh magnus@login.toolforge.org -L 3308:tools-db:3306 -N &
ssh magnus@login.toolforge.org -L 3309:wikidatawiki.web.db.svc.wikimedia.cloud:3306 -N &
The web service reads from the ToolsDB replica, on the `toolsdb_readonly` tunnel port:
ssh magnus@login.toolforge.org -L 3310:tools-readonly.db.svc.wikimedia.cloud:3306 -N &
*/
