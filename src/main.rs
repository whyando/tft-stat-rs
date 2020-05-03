#[macro_use] extern crate log;
use std::collections::VecDeque;
use tokio;
use futures::future::FutureExt;
use bson;
use bson::doc;
use serde_json::json;
use serde_json;
use std::sync::Arc;

use riven::{RiotApi, RiotApiConfig};
use riven::consts::Region;
use riven::models::tft_league_v1::LeagueList;
use mongodb::{Client, options::ClientOptions};
use mongodb::options::FindOptions;

use promise_buffer::promise_buffer;

mod promise_buffer;

#[tokio::main]
async fn main() -> () {
    env_logger::init();

    let db_connection_string = std::env::var("DB_CONNECTION_STRING").expect("Missing environment variable: DB_CONNECTION_STRING");
    let api_key = std::env::var("RGAPI_KEY").expect("Missing environment variable: RGAPI_KEY");
    let api_config = RiotApiConfig::with_key(api_key).preconfig_throughput();
    let api = Arc::new(RiotApi::with_config(api_config));
    
    let mut client_options = ClientOptions::parse(&db_connection_string).unwrap();
    client_options.app_name = Some("tft_stat".to_string());
    let client = Client::with_options(client_options).unwrap();
    let db = Arc::new(client.database("tft"));
    
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
    db: Arc<mongodb::Database>
}

impl Main {
    async fn do_cycle(&self) {
        info!("[{}] Main begin.", self.region);
        let summoner_list = self.get_top_players().await;
        info!("[{}] Gathered summoner ids for {} players.", self.region, summoner_list.len());

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

        info!("[{}] Main Done.", self.region);
    }

    async fn process_summoner_id(&self, index: usize, id: &str) -> anyhow::Result<()> {
        let player = self.api.summoner_v4().get_by_summoner_id(self.region, id).await?;
        let player_match = self.api.tft_match_v1().get_match_ids_by_puuid(self.region_major, &player.puuid, Some(10)).await?;

        let mut new: i32 = 0;
        let mut repeat: i32 = 0;
        let mut new_error: i32 = 0;
        for x in &player_match {
            match self.process_match_id(&x).await {
                Err(e) =>  error!("{:#?}", e),
                Ok(-1) => new_error += 1,
                Ok(0) => repeat += 1,
                Ok(1) => new += 1,
                Ok(_) => unreachable!(),
            }
        }
        debug!("{} {} {:#?} {} ({} New, {} Old, {} Error)", index, self.region, player.name, player_match.len(), new, repeat, new_error);
        Ok(())
    }

    async fn process_match_id(&self, id: &str) -> anyhow::Result<i64>{
        let matches = self.db.collection("matches_v2");
        let filter = doc! {"_id": id};
        let find_options = FindOptions::default();
        let cursor = matches.find(filter, find_options)?;

        let ret: Vec<_> = cursor.collect();
        // debug!("{:#?}", ret);

        if ret.len() != 0 {
            return Ok(0);
        }

        let game = match self.api.tft_match_v1().get_match(self.region_major, id).await {
            Ok(g) => g,
            Err(e) => {
                // let req_err = e.source_reqwest_error().to_string();
                error!("Error on GET_MATCH({},{}): {}", self.region_major, id, e);
                None
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
            ("GRANDMASTER", "I"),
            ("MASTER", "I"),
            ("DIAMOND", "I"),
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


