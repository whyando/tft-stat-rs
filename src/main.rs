#[macro_use]
extern crate log;
use bson;
use bson::doc;
use futures::future::FutureExt;
use serde_json;
use serde_json::json;
use std::collections::VecDeque;
use std::iter::Iterator;
use std::sync::Arc;
use tokio;

use mongodb::options::FindOneOptions;
use mongodb::{options::ClientOptions, Client};
use riven::consts::Region;
use riven::models::tft_league_v1::LeagueList;
use riven::{RiotApi, RiotApiConfig};

use promise_buffer::promise_buffer;

mod promise_buffer;

#[tokio::main]
async fn main() -> () {
    env_logger::init();

    let api = {
        let api_key = std::env::var("RGAPI_KEY").expect("Missing environment variable: RGAPI_KEY");
        let api_config = RiotApiConfig::with_key(api_key).preconfig_throughput();
        Arc::new(RiotApi::with_config(api_config))
    };

    let db = {
        let db_connection_string = std::env::var("DB_CONNECTION_STRING")
            .expect("Missing environment variable: DB_CONNECTION_STRING");
        let mut client_options = ClientOptions::parse(&db_connection_string)
            .await
            .expect("Unable to parse DB options");
        client_options.app_name = Some("tft_stat".to_string());
        let client = Client::with_options(client_options).expect("Unable to construct DB client");
        Arc::new(client.database("tft"))
    };

    let m1 = Main {
        region: Region::NA,
        region_major: Region::AMERICAS,
        api: api.clone(),
        db: db.clone(),
    };
    let m2 = Main {
        region: Region::EUW,
        region_major: Region::EUROPE,
        api: api.clone(),
        db: db.clone(),
    };
    let m3 = Main {
        region: Region::KR,
        region_major: Region::ASIA,
        api: api.clone(),
        db: db.clone(),
    };
    let s1 = tokio::spawn(async move {
        loop {
            m1.do_cycle().await;
        }
    });
    let s2 = tokio::spawn(async move {
        loop {
            m2.do_cycle().await;
        }
    });
    let s3 = tokio::spawn(async move {
        loop {
            m3.do_cycle().await;
        }
    });
    s1.await.expect("The task being joined has panicked");
    s2.await.expect("The task being joined has panicked");
    s3.await.expect("The task being joined has panicked");
}

struct Main {
    api: Arc<RiotApi>,
    region: Region,
    region_major: Region,
    db: Arc<mongodb::Database>,
}

impl Main {
    async fn do_cycle(&self) {
        info!("[{}] Main begin.", self.region);
        let summoner_list = self.get_top_players().await;
        info!(
            "[{}] Gathered summoner ids for {} players.",
            self.region,
            summoner_list.len()
        );

        // VecDeque of Futures
        let q: VecDeque<_> = summoner_list
            .iter()
            .enumerate()
            .map(|(index, id)| self.process_summoner_id(index, id).boxed())
            .collect();

        promise_buffer(q, 1, |result| match result {
            Ok(_) => {}
            Err(e) => {
                error!("{:#?}", e);
            }
        })
        .await;

        info!("[{}] Main Done.", self.region);
    }

    /// Do all processing for a single summoner
    /// Propagates up errors from database and api calls (but not match fetching errors)
    async fn process_summoner_id(&self, index: usize, id: &str) -> anyhow::Result<()> {
        let player = self
            .api
            .tft_summoner_v1()
            .get_by_summoner_id(self.region, id)
            .await?;
        let player_match = self
            .api
            .tft_match_v1()
            .get_match_ids_by_puuid(self.region_major, &player.puuid, Some(10))
            .await?;

        let mut new: i32 = 0;
        let mut repeat: i32 = 0;
        let mut new_error: i32 = 0;
        for x in &player_match {
            match self.process_match_id(&x).await {
                Err(e) => error!("{:#?}", e),
                Ok(-1) => new_error += 1,
                Ok(0) => repeat += 1,
                Ok(1) => new += 1,
                Ok(_) => unreachable!(),
            }
        }
        debug!(
            "{} {} {:#?} {} ({} New, {} Old, {} Error)",
            index,
            self.region,
            player.name,
            player_match.len(),
            new,
            repeat,
            new_error
        );
        Ok(())
    }

    async fn process_match_id(&self, id: &str) -> anyhow::Result<i64> {
        let matches = self.db.collection("matches_v2");
        let filter = doc! {"_id": id};
        let find_options = FindOneOptions::default();
        let doc = matches.find_one(filter, find_options).await?;

        if let Some(_) = doc {
            return Ok(0);
        }

        let game = match self
            .api
            .tft_match_v1()
            .get_match(self.region_major, id)
            .await
        {
            Ok(g) => g,
            Err(e) => {
                // let req_err = e.source_reqwest_error().to_string();
                error!("Error on GET_MATCH({},{}): {}", self.region_major, id, e);
                None
            }
        };

        let mut game_json = match &game {
            None => json!({}),
            Some(g) => serde_json::to_value(g)?,
        };
        game_json["_id"] = json!(id);
        let bson: bson::Bson = game_json.into();

        let doc = bson
            .as_document()
            .ok_or_else(|| anyhow::Error::msg("Error creating mongo doc"))?;
        matches.insert_one(doc.clone(), None).await?;

        match &game {
            None => Ok(-1),
            Some(_) => Ok(1),
        }
    }

    async fn get_top_players(&self) -> Vec<String> {
        let mut ret = Vec::new();

        for (tier, division) in &[
            ("CHALLENGER", "I"),
            ("GRANDMASTER", "I"),
            ("MASTER", "I"),
            ("DIAMOND", "I"),
            // ("DIAMOND", "II"),
            // ("DIAMOND", "III"),
            // ("DIAMOND", "IV"),
        ] {
            let mut entries = {
                let mut x = self.get_league_entries(tier, division).await;
                let mut num_failures: i32 = 0;
                while let Err(e) = &x {
                    error!("Error get_league_entries {} {}: {}", tier, division, e);
                    num_failures += 1;
                    if num_failures == 5 {
                        break;
                    }
                    tokio::time::delay_for(std::time::Duration::from_secs(20)).await;
                    x = self.get_league_entries(tier, division).await;
                }
                x.expect("Too many failures")
            };
            info!("{} {} {}\t{}", self.region, tier, division, entries.len());
            ret.append(&mut entries);
        }
        ret
    }

    async fn get_league_entries(&self, tier: &str, division: &str) -> anyhow::Result<Vec<String>> {
        // non-paginated cases
        let x: Option<LeagueList> = match tier {
            "CHALLENGER" => Some(
                self.api
                    .tft_league_v1()
                    .get_challenger_league(self.region)
                    .await?,
            ),
            "GRANDMASTER" => Some(
                self.api
                    .tft_league_v1()
                    .get_grandmaster_league(self.region)
                    .await?,
            ),
            "MASTER" => Some(
                self.api
                    .tft_league_v1()
                    .get_master_league(self.region)
                    .await?,
            ),
            _ => None,
        };
        if let Some(ll) = x {
            let summoner_id_list = ll.entries.iter().map(|y| y.summoner_id.clone()).collect();
            return Ok(summoner_id_list);
        }

        // paginated cases
        let mut page = 1;
        let mut ret = Vec::new();
        loop {
            let x = self
                .api
                .tft_league_v1()
                .get_league_entries(self.region, tier, division, Some(page))
                .await?;
            if x.len() == 0 {
                break;
            };

            for y in x {
                ret.push(y.summoner_id.clone());
            }
            page += 1;
        }
        Ok(ret)
    }
}
