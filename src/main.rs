#[macro_use]
extern crate log;

mod numeric_league_util;

use chrono::offset::TimeZone;
use chrono::offset::Utc;
use chrono::Duration;
use futures::stream::FuturesUnordered;
use futures::stream::StreamExt;
use mongodb::bson::document::Document;
use mongodb::bson::{doc, Bson};
use serde_json;
use std::collections::VecDeque;
use std::convert::TryInto;
use std::iter::Iterator;
use std::sync::Arc;
use tokio;
use tokio::time::delay_for as sleep;

use mongodb::options::{ClientOptions, CountOptions, FindOneOptions};
use mongodb::Client;
use riven::consts::Region;
use riven::models::tft_league_v1::LeagueList;
use riven::{RiotApi, RiotApiConfig};

use numeric_league_util::{league_to_numeric, team_avg_rank_str};

const MATCHES_COLLECTION_NAME: &str = "matches-4-1";
const SUMMONERS_COLLECTION_NAME: &str = "summoner-4-1";
const LEAGUES_COLLECTION_NAME: &str = "league-4-1";

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

    let mut join_handles = vec![];

    for (region, region_major) in &[
        (Region::EUW, Region::EUROPE),
        (Region::EUNE, Region::EUROPE),
        (Region::KR, Region::ASIA),
        (Region::JP, Region::ASIA),
        (Region::NA, Region::AMERICAS),
        (Region::BR, Region::AMERICAS),
        (Region::OCE, Region::AMERICAS),
    ] {
        let api_clone = api.clone();
        let db_clone = db.clone();
        let hdl = tokio::spawn(async move {
            Main {
                region: *region,
                region_major: *region_major,
                api: api_clone,
                db: db_clone,
            }
            .run()
            .await;
        });
        join_handles.push(hdl);
    }
    let (_i, idx, _v) = futures::future::select_all(join_handles).await;
    panic!(format!("Handle {} returned.", idx));
}

#[derive(Clone)]
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

        let mut q: VecDeque<(usize, &String)> = summoner_list.iter().enumerate().collect();

        let mut futures = FuturesUnordered::new();
        loop {
            if q.is_empty() && futures.is_empty() {
                break;
            }
            while !q.is_empty() && futures.len() < 10 {
                futures.push(
                    q.pop_front()
                        .map(|(index, id)| self.process_summoner_id(index, id))
                        .unwrap(),
                );
                sleep(tokio::time::Duration::from_millis(2000)).await;
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
        let num_doc = matches
            .count_documents(filter, count_options)
            .await
            .map_err(|_| anyhow::Error::msg("Error counting documents"))?;

        if num_doc != 0 {
            return Ok(0);
        }

        let current_timestamp = Utc::now();
        // Fetch details of the match
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
                // Get information about the participants in this game
                let (player_data, avg_elo, avg_elo_text) =
                    self.get_extended_participant_info(&game).await?;

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

                doc.insert("_aggregatedPlayerInfo", player_data);
                doc.insert("_avgElo", avg_elo);
                doc.insert("_avgEloText", avg_elo_text);

                matches
                    .insert_one(doc.clone(), None)
                    .await
                    .map_err(|_| anyhow::Error::msg("Error inserting document"))?;
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
                matches
                    .insert_one(doc.clone(), None)
                    .await
                    .map_err(|_| anyhow::Error::msg("Error inserting document"))?;
                Ok(-1)
            }
        }
    }

    async fn get_extended_participant_info(
        &self,
        game: &riven::models::tft_match_v1::Match,
    ) -> anyhow::Result<(Vec<Bson>, i32, String)> {
        let mut ret: Vec<Bson> = vec![];
        let mut sum = 0;
        let mut num_ranked = 0;

        let mut ranks_vec = vec![];

        for puuid in &game.metadata.participants {
            // 1. parse 8 puuids
            trace!("puuid {:?}", puuid);

            // 2. get 8 summonerIds (cached or riot query)
            let summoner_doc = self
                .tft_summoner_v1(puuid)
                .await
                .map_err(|_| anyhow::Error::msg("Error tft_summoner_v1"))?;
            let summoner_id = summoner_doc.get_str("id")?;
            trace!("{}", summoner_id);

            // 3. get 8 tft league entries (cached or riot query)
            let league_doc = self
                .tft_league_v1(summoner_id)
                .await
                .map_err(|_| anyhow::Error::msg("Error tft_league_v1"))?;
            let tft_tier = league_doc.get_str("tier").unwrap_or("unranked");
            let tft_rank = league_doc.get_str("rank").unwrap_or("unranked");
            let tft_league_points = league_doc.get_i32("leaguePoints").unwrap_or(i32::MIN);

            ranks_vec.push((
                tft_tier.to_string(),
                tft_rank.to_string(),
                tft_league_points,
            ));

            // 4. construct object to append to the game with all known info
            let aggregated_doc = doc! {
                "summonerId": summoner_id,
                "summonerName": summoner_doc.get_str("name")?,
                "accountId": summoner_doc.get_str("accountId")?,
                "puuid": puuid,
                "tftTier": tft_tier,
                "tftRank": tft_rank,
                "tftLeaguePoints": tft_league_points,
            };
            ret.push(aggregated_doc.into());

            let league_status = league_doc.get_str("_status")?;
            if league_status == "ranked" {
                sum += league_to_numeric(tft_tier, tft_rank, tft_league_points);
                num_ranked += 1;
            }
        }
        let (avg_elo, avg_elo_str) = if num_ranked == 8 {
            (sum / 8, team_avg_rank_str(&ranks_vec))
        } else {
            (i32::MIN, "UNRANKED".to_string())
        };
        Ok((ret, avg_elo, avg_elo_str))
    }

    // puuid -> summoner doc
    async fn tft_summoner_v1(&self, puuid: &str) -> anyhow::Result<Document> {
        let summoners = self.db.collection(SUMMONERS_COLLECTION_NAME);
        let filter = doc! {"_id": puuid};

        let find_options = FindOneOptions::default();
        let current_timestamp = Utc::now();
        let doc = match summoners
            .find_one(filter, find_options)
            .await
            .map_err(|_| anyhow::Error::msg("Error find_one"))?
        {
            None => {
                let tft_summoner = self
                    .api
                    .tft_summoner_v1()
                    .get_by_puuid(self.region, puuid)
                    .await?;
                let mut bson: Bson = serde_json::to_value(tft_summoner)?.try_into()?;
                let doc = bson
                    .as_document_mut()
                    .ok_or_else(|| anyhow::Error::msg("BSON is not a doc"))?;
                doc.insert("_id", Bson::String(puuid.to_string()));
                doc.insert("_documentCreated", Bson::DateTime(current_timestamp));
                // Don't expire this document for 60 days
                let expire = current_timestamp + Duration::days(30);
                doc.insert("_documentExpire", Bson::DateTime(expire));
                summoners
                    .insert_one(doc.clone(), None)
                    .await
                    .map_err(|_| anyhow::Error::msg("Error inserting document"))?;
                // debug!("summoner (new)");
                doc.clone()
            }
            Some(doc) => {
                // debug!("summoner (cached)");
                doc
            }
        };
        Ok(doc)
    }

    // summonerId -> league doc
    async fn tft_league_v1(&self, summoner_id: &str) -> anyhow::Result<Document> {
        let leagues = self.db.collection(LEAGUES_COLLECTION_NAME);
        let filter = doc! {"_id": summoner_id};

        let find_options = FindOneOptions::default();
        let current_timestamp = Utc::now();
        let doc = match leagues
            .find_one(filter, find_options)
            .await
            .map_err(|_| anyhow::Error::msg("Error find one"))?
        {
            None => {
                let tft_league_vec = self
                    .api
                    .tft_league_v1()
                    .get_league_entries_for_summoner(self.region, summoner_id)
                    .await?;
                #[allow(deprecated)] // riven::consts::QueueType::RANKED_TFT is marked deprecated
                let tft_league_opt = tft_league_vec
                    .iter()
                    .find(|item| item.queue_type == riven::consts::QueueType::RANKED_TFT);
                let mut doc = if let Some(tft_league) = tft_league_opt {
                    // debug!("leagues (found)");
                    let mut bson: Bson = serde_json::to_value(tft_league)?.try_into()?;
                    let doc = bson
                        .as_document_mut()
                        .ok_or_else(|| anyhow::Error::msg("BSON is not a doc"))?;
                    doc.insert("_status", Bson::String("ranked".to_string()));
                    doc.clone()
                } else {
                    // debug!("leagues (not found)");
                    let mut doc = doc! {};
                    doc.insert("_status", Bson::String("unranked".to_string()));
                    doc
                };
                doc.insert("_id", Bson::String(summoner_id.to_string()));
                doc.insert("_documentCreated", Bson::DateTime(current_timestamp));
                // Don't expire this document for 1 days
                let expire = current_timestamp + Duration::days(1);
                doc.insert("_documentExpire", Bson::DateTime(expire));
                leagues
                    .insert_one(doc.clone(), None)
                    .await
                    .map_err(|_| anyhow::Error::msg("Error inserting document"))?;
                doc
            }
            Some(doc) => {
                // debug!("leagues (cached)");
                doc
            }
        };
        // debug!("{:}", doc);
        Ok(doc)
    }

    // Returns a list of summoner ids
    async fn get_top_players(&self) -> Vec<String> {
        let mut ret = Vec::new();

        // TODO: make divisions configurable
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
            // ("PLATINUM", "IV"),
            // ("GOLD", "I"),
            // ("GOLD", "II"),
            // ("GOLD", "III"),
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
                    sleep(tokio::time::Duration::from_secs(20)).await;
                    x = self.get_league_entries(tier, division).await;
                }
                x.expect("Too many failures")
            };
            info!("{} {} {}\t{}", self.region, tier, division, entries.len());
            ret.append(&mut entries);
        }
        ret
    }

    // Returns a list of summoner ids
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
                .await
                .map_err(|_| anyhow::Error::msg("Error get_league_entries"))?;
            if x.len() == 0 {
                break;
            };

            // Here we have the list of entries, which we distill down to a list of summoner ids
            for y in x {
                ret.push(y.summoner_id.clone());
                /*
                globally we know:
                    tier = "PLATINUM"
                    division="I"

                for this specific entry:
                    y.leaguePoints="266"
                    y.rank="I" (same as above I hope)

                identity:
                    y.summonerId
                    y.summonerName
                    NOT: puuid or accountId
                */
                // We may want to use this ranking to update DB knowledge about this player
                // (it is indexed on summonerId)
            }
            page += 1;
        }
        Ok(ret)
    }
}
