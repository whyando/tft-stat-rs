#[macro_use] extern crate log;
use std::collections::VecDeque;
use tokio;
use futures::future::FutureExt;
use bson;
use bson::doc;
use serde_json::json;
use serde_json;

use riven::RiotApi;
use riven::consts::Region;
use riven::models::tft_league_v1::LeagueList;
use mongodb::{Client, options::ClientOptions};
use mongodb::options::FindOptions;

use promise_buffer::promise_buffer;

mod promise_buffer;

const DECODE_ERROR: &str = "error decoding response body: missing field `tier_total`";

#[tokio::main]
async fn main() -> () {
    env_logger::init();

    let api_key = std::env::var("API_KEY").unwrap();
    let api = RiotApi::with_key(api_key);
    
    let mut client_options = ClientOptions::parse("mongodb://localhost:27017").unwrap();
    client_options.app_name = Some("My App".to_string());
    let client = Client::with_options(client_options).unwrap();
    let db = client.database("tft");

    Main {
        region: Region::EUW,
        region_major: Region::EUROPE,
        api,
        db,
    }.do_cycle().await;
}

struct Main {
    api: RiotApi,    
    region: Region,
    region_major: Region,
    db: mongodb::Database
}

impl Main {
    async fn do_cycle(&self) {
        info!("Main begin.");
        let summoner_list = self.get_top_players().await;
        info!("Gathered summoner ids for {} players.", summoner_list.len());

        // VecDeque of Futures
        let q: VecDeque<_> = summoner_list.iter().enumerate().map(|(index, id)| self.process_summoner_id(index, id).boxed()).collect();
        
        promise_buffer(q, 1, |result| {
            match result {
                Ok(_) => {}
                Err(e) => {
                    error!("{:#?}", e);
                },
            }
        }).await;

        info!("Main Done.");
    }

    async fn process_summoner_id(&self, index: usize, id: &str) -> anyhow::Result<()> {
        let player = self.api.summoner_v4().get_by_summoner_id(self.region, id).await?;
        let player_match = self.api.tft_match_v1().get_match_ids_by_puuid(self.region_major, &player.puuid, Some(20)).await?;

        let mut new_games = 0;
        let mut repeat_games = 0;
        let mut new_bugged = 0;
        for x in &player_match {
            match self.process_match_id(&x).await {
                Err(e) =>  error!("{:#?}", e),
                Ok(-1) => new_bugged += 1,
                Ok(0) => repeat_games += 1,
                Ok(1) => new_games += 1,
                Ok(_) => unreachable!(),
            }
        }
        debug!("{} {} {:#?} {} ({} New, {} Old, {} Bugged)", index, self.region, player.name, player_match.len(), new_games, repeat_games, new_bugged);
        Ok(())
    }

    async fn process_match_id(&self, id: &str) -> anyhow::Result<i64>{
        let matches = self.db.collection("matches");
        let filter = doc! {"_id": id};
        let find_options = FindOptions::default();
        let cursor = matches.find(filter, find_options)?;

        let ret: Vec<_> = cursor.collect();
        // debug!("{:#?}", ret);

        if ret.len() != 0 {
            return Ok(0);
        }

        let game = match self.api.tft_match_v1().get_match(self.region_major, id).await {
            Ok(g) => Some(g),
            Err(e) => {
                let req_err = e.source_reqwest_error().to_string();
                if req_err.starts_with(DECODE_ERROR) {
                    warn!("Decode error on GET_MATCH({},{})", self.region_major, id);
                    None
                } else {
                    error!("Error on GET_MATCH({},{})", self.region_major, id);
                    None
                }
            }
        };

        let mut game_json = match &game {
            None => json!({}),
            Some(g) =>  serde_json::to_value(g).unwrap()
        };
        game_json["_id"] = json!(id);
        let bson: bson::Bson = game_json.into();
        let doc = bson.as_document().unwrap();
        matches.insert_one(doc.clone(), None)?;

        match &game {
            None => Ok(-1),
            Some(_) => Ok(1),
        }
    }

    async fn get_top_players(&self) -> Vec<String> {
        let mut ret = Vec::new();
    
        for (tier, division) in &[
            ("CHALLENGER", "I"),
            // ("GRANDMASTER", "I"),
            // ("MASTER", "I"),
            // ("DIAMOND", "I"),
            // ("DIAMOND", "II"),
            // ("DIAMOND", "III"),
            // ("DIAMOND", "IV"),
        ] {
            let mut x = self.get_league_entries(tier, division).await;
            info!("{} {} {}\t{}", self.region, tier, division, x.len());
            ret.append(&mut x);
        }
        ret
    }
    
    async fn get_league_entries(&self, tier: &str, division: &str) -> Vec<String> {
        // special cases
        let x: Option<LeagueList> = match tier {
            "CHALLENGER" => Some(self.api.tft_league_v1().get_challenger_league(self.region).await
                .expect("get_challenger_league failed")),
            "GRANDMASTER" => Some(self.api.tft_league_v1().get_grandmaster_league(self.region).await
                .expect("get_grandmaster_league failed")),
            "MASTER" => Some(self.api.tft_league_v1().get_master_league(self.region).await
                .expect("get_master_league failed")),
            _ => None,
        };
        if !x.is_none() {
            let x = x.unwrap();
            return x.entries.iter().map(|y| y.summoner_id.clone()).collect();
        }
    
        let mut page = 1;
        let mut ret = Vec::new();
        loop {
            let x = self.api.tft_league_v1().get_league_entries(self.region, tier, division, Some(page)).await
                .expect("get_league_entries failed");        
            if x.len() == 0 { break };
    
            for y in x {
                ret.push(y.summoner_id.clone());
            }
            page+=1;
        }
        ret 
    }
}


