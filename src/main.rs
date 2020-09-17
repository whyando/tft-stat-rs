#![type_length_limit = "3104818"]
#[macro_use]
extern crate log;

use chrono::offset::TimeZone;
use chrono::offset::Utc;
use chrono::Duration;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use mongodb::bson::doc;
use mongodb::bson::Bson;
use serde_json;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::iter::Iterator;
use std::sync::Arc;
use tokio;

use mongodb::options::{ClientOptions, CountOptions};
use mongodb::Client;
use riven::consts::Region;
use riven::models::tft_league_v1::LeagueList;
use riven::{RiotApi, RiotApiConfig};

const MATCHES_COLLECTION_NAME: &str = "matches_v3";

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
    let m4 = Main {
        region: Region::JP,
        region_major: Region::ASIA,
        api: api.clone(),
        db: db.clone(),
    };
    let m5 = Main {
        region: Region::BR,
        region_major: Region::AMERICAS,
        api: api.clone(),
        db: db.clone(),
    };

    futures::join!(m1.run(), m2.run(), m3.run(), m4.run(), m5.run());
}

struct Main {
    api: Arc<RiotApi>,
    region: Region,
    region_major: Region,
    db: Arc<mongodb::Database>,
}

impl Main {
    // run forever
    async fn run(&self) {
        loop {
            self.do_cycle().await;
        }
    }

    async fn do_cycle(&self) {
        info!("[{}] Main begin.", self.region);
        let summoner_list = self.get_top_players().await;
        info!(
            "[{}] Gathered summoner ids for {} players.",
            self.region,
            summoner_list.len()
        );

        // VecDeque of Futures
        let mut q: VecDeque<_> = summoner_list
            .iter()
            .enumerate()
            //.map(|(index, id)| self.process_summoner_id(index, id)) //.boxed())
            .collect();

        let mut futures = FuturesUnordered::new();
        loop {
            if q.is_empty() && futures.is_empty() {
                break;
            }
            if !q.is_empty() && futures.len() < 3 {
                futures.push(
                    q.pop_front()
                        .map(|(index, id)| self.process_summoner_id(index, id))
                        .unwrap(),
                );
            }
            match futures.next().await {
                Some(_ret) => (),
                None => break,
            }
        }

        info!("[{}] Main Done.", self.region);
    }

    /// Do all processing for a single summoner
    /// Propagates up errors from database and api calls (but not match fetching errors)
    async fn process_summoner_id(&self, index: usize, id: &str) {
        let player = self
            .api
            .tft_summoner_v1()
            .get_by_summoner_id(self.region, id)
            .await;
        let player = match player {
            Ok(player) => player,
            Err(e) => return error!("tft_summoner_v1 error: {}", e.to_string()),
        };
        let player_match = self
            .api
            .tft_match_v1()
            .get_match_ids_by_puuid(self.region_major, &player.puuid, Some(10))
            .await;
        let player_match = match player_match {
            Ok(player_match) => player_match,
            Err(e) => return error!("tft_match_v1 error: {}", e.to_string()),
        };

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
    }

    async fn process_match_id(&self, id: &str) -> anyhow::Result<i64> {
        let matches = self.db.collection(MATCHES_COLLECTION_NAME);
        let filter = doc! {"_id": id};
        let count_options = CountOptions::default();
        let num_doc = matches.count_documents(filter, count_options).await?;

        if num_doc != 0 {
            return Ok(0);
        }

        let current_timestamp = Utc::now();
        match self
            .api
            .tft_match_v1()
            .get_match(self.region_major, id)
            .await
            .unwrap_or_else(|e| {
                // let req_err = e.source_reqwest_error().to_string();
                error!("Error on GET_MATCH({},{}): {}", self.region_major, id, e);
                None
            }) {
            Some(game) => {
                let match_timestamp = Utc.timestamp_millis(game.info.game_datetime);
                let mut bson: Bson = serde_json::to_value(game)?.try_into()?;
                let doc = bson
                    .as_document_mut()
                    .ok_or_else(|| anyhow::Error::msg("BSON is not a doc"))?;
                doc.insert("_id", Bson::String(id.to_string()));
                doc.insert("_documentCreated", Bson::DateTime(current_timestamp));
                doc.insert("_matchTimestamp", Bson::DateTime(match_timestamp));
                // Don't expire this document until the game date was 7 days ago
                // Additionally don't expire within the next 24 hours
                let expire = std::cmp::max(
                    current_timestamp + Duration::hours(24),
                    match_timestamp + Duration::days(7),
                );
                doc.insert("_documentExpire", Bson::DateTime(expire));
                matches.insert_one(doc.clone(), None).await?;
                Ok(1)
            }
            None => {
                // Insert a dummy document, so we don't keep trying to fetch this game
                let mut doc = doc! {};
                doc.insert("_id", Bson::String(id.to_string()));
                doc.insert("_documentCreated", Bson::DateTime(current_timestamp));
                // Expire document 24 hours after creation
                doc.insert(
                    "_documentExpire",
                    Bson::DateTime(current_timestamp + Duration::hours(24)),
                );
                matches.insert_one(doc.clone(), None).await?;
                Ok(-1)
            }
        }
    }

    async fn get_top_players(&self) -> Vec<String> {
        let mut ret = Vec::new();

        for (tier, division) in &[
            // ("CHALLENGER", "I"),
            // ("GRANDMASTER", "I"),
            // ("MASTER", "I"),
            ("DIAMOND", "I"),
            ("DIAMOND", "II"),
            ("DIAMOND", "III"),
            ("DIAMOND", "IV"),
            ("PLATINUM", "I"),
            ("PLATINUM", "II"),
            ("PLATINUM", "III"),
            ("PLATINUM", "IV"),
            ("GOLD", "I"),
            ("GOLD", "II"),
            ("GOLD", "III"),
            // ("GOLD", "IV"),
            // ("SILVER", "I"),
            // ("SILVER", "II"),
            // ("SILVER", "III"),
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
