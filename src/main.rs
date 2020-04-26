#[macro_use] extern crate log;
use tokio;
use futures::future::FutureExt;

use riven::RiotApi;
use riven::consts::Region;
use riven::models::tft_league_v1::LeagueList;

use promise_buffer::promise_buffer;

mod promise_buffer;

#[tokio::main]
async fn main() -> () {
    env_logger::init();
    Main {
        region: Region::EUW,
        region_major: Region::EUROPE,
        api: RiotApi::with_key("RGAPI-bef235e0-609a-424e-b7ab-49ed8ac54c87"),
    }.do_cycle().await;
}

struct Main {
    api: RiotApi,    
    region: Region,
    region_major: Region,
}

impl Main {
    async fn do_cycle(&self) {
        info!("Main begin.");
        let summoner_list = self.get_top_players().await;
        info!("Gathered summoner ids for {} players.", summoner_list.len());

        // Vec of Futures
        let mut q: Vec<_> = summoner_list.iter().map(|id| self.process_summoner_id(id).boxed()).collect();
        
        let mut game_id_list: Vec<String> = Vec::new();
        
        promise_buffer(q, 10,
           |mut r| {
               match r {
                    Ok(ref mut v) => game_id_list.append(v),
                    Err(e) => error!("{:#?}", e)
               }
           },
        ).await;

        // Now process game_id_list
        game_id_list.sort();
        game_id_list.dedup();

        let mut q: Vec<_> = game_id_list.iter().map(|id| self.process_match_id(id).boxed()).collect();
    
        info!("Main Done.");
    }

    async fn process_match_id(&self, id: &str) -> anyhow::Result<()>{
        Ok(())
    }

    async fn process_summoner_id(&self, id: &str) -> anyhow::Result<Vec<String>> {
        let player = self.api.summoner_v4().get_by_summoner_id(self.region, id).await?;
        let player_match = self.api.tft_match_v1().get_match_ids_by_puuid(self.region_major, &player.puuid, Some(20)).await?;
        debug!("{:#?} {}", player.name, player_match.len());
        Ok(player_match)
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


