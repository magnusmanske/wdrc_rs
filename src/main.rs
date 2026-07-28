mod change;
mod recent_changes;
mod revision_compare;
mod wdrc;

use std::env;
use wdrc::*;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();

    let command = args.get(1).expect("command required");

    let config_file = args
        .get(2)
        .map(|s| s.to_string())
        .unwrap_or("config.json".to_string());
    let mut wdrc = WdRc::new(&config_file);

    if command == "bot" {
        let sleep_duration = wdrc.bot_sleep();
        loop {
            match wdrc.run_once().await {
                Ok(RunResult::MoreWork) => continue,
                Ok(RunResult::CaughtUp) => (),
                Err(e) => eprintln!("Error: {}", e),
            }
            tokio::time::sleep(sleep_duration).await;
        }
    } else if command == "run" {
        match wdrc.run_once().await {
            Ok(_) => (),
            Err(e) => eprintln!("Error: {}", e),
        }
    }
}

/* TESTING
On Toolforge, credentials are read from ~/replica.my.cnf by the `toolforge` crate.
For local testing, open SSH tunnels and add an explicit `url` to the respective
config section, which overrides the ~/replica.my.cnf lookup:
ssh magnus@login.toolforge.org -L 3308:tools-db:3306 -N &
ssh magnus@login.toolforge.org -L 3309:wikidatawiki.web.db.svc.wikimedia.cloud:3306 -N &
*/
