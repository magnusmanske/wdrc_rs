mod change;
mod recent_changes;
mod revision_compare;
mod wdrc;
mod wikidata_api;

use anyhow::{anyhow, Result};
use std::env;
use wdrc::*;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let command = args
        .get(1)
        .ok_or_else(|| anyhow!("Usage: wdrc_rs <bot|run> [config.json]"))?;
    let config_file = args.get(2).map(|s| s.as_str()).unwrap_or("config.json");
    let mut wdrc = WdRc::new(config_file)?;

    match command.as_str() {
        "bot" => {
            let sleep_duration = wdrc.bot_sleep();
            loop {
                match wdrc.run_once().await {
                    // More work waiting: start the next batch immediately.
                    Ok(RunResult::MoreWork) => continue,
                    Ok(RunResult::CaughtUp) => (),
                    Err(e) => eprintln!("Error: {e}"),
                }
                tokio::time::sleep(sleep_duration).await;
            }
        }
        "run" => {
            wdrc.run_once().await?;
            Ok(())
        }
        other => Err(anyhow!("Unknown command {other:?}, expected 'bot' or 'run'")),
    }
}

/* TESTING
On Toolforge, credentials are read from ~/replica.my.cnf by the `toolforge` crate.
For local testing, open SSH tunnels and add an explicit `url` to the respective
config section, which overrides the ~/replica.my.cnf lookup:
ssh magnus@login.toolforge.org -L 3308:tools-db:3306 -N &
ssh magnus@login.toolforge.org -L 3309:wikidatawiki.web.db.svc.wikimedia.cloud:3306 -N &
*/
