pub fn league_to_numeric(tier: &str, rank: &str, league_points: i32) -> i32 {
    let base = match tier {
        "IRON" => 0,
        "BRONZE" => 400,
        "SILVER" => 800,
        "GOLD" => 1200,
        "PLATINUM" => 1600,
        "DIAMOND" => 2000,
        "MASTER" => 2400,
        "GRANDMASTER" => 2400,
        "CHALLENGER" => 2400,
        _ => panic!(),
    };
    let rank_addition = if !(tier == "MASTER" || tier == "GRANDMASTER" || tier == "CHALLENGER") {
        match rank {
            "IV" => 0,
            "III" => 100,
            "II" => 200,
            "I" => 300,
            _ => panic!(),
        }
    } else {
        0
    };
    base + rank_addition + league_points
}

// One day...
// fn numeric_to_league(x: i32) -> String {
//     String::default()
// }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_league_to_numeric() {
        assert_eq!(league_to_numeric("IRON", "IV", -21), -21);
        assert_eq!(league_to_numeric("IRON", "IV", 0), 0);
        assert_eq!(league_to_numeric("BRONZE", "II", 54), 654);
        assert_eq!(league_to_numeric("SILVER", "I", 16), 1116);
        assert_eq!(league_to_numeric("GOLD", "IV", 0), 1200);
        assert_eq!(league_to_numeric("GOLD", "III", 50), 1350);
        assert_eq!(league_to_numeric("GOLD", "III", 100), 1400);
        assert_eq!(league_to_numeric("GOLD", "II", 0), 1400);
        assert_eq!(league_to_numeric("PLATINUM", "III", 31), 1731);
        assert_eq!(league_to_numeric("PLATINUM", "III", -32), 1668);

        assert_eq!(league_to_numeric("DIAMOND", "IV", 0), 2000);
        assert_eq!(league_to_numeric("DIAMOND", "III", 0), 2100);
        assert_eq!(league_to_numeric("DIAMOND", "II", 0), 2200);
        assert_eq!(league_to_numeric("DIAMOND", "I", 0), 2300);
        assert_eq!(league_to_numeric("DIAMOND", "I", 99), 2399);
        assert_eq!(league_to_numeric("MASTER", "I", 0), 2400);

        assert_eq!(league_to_numeric("MASTER", "I", 1), 2401);
        assert_eq!(league_to_numeric("GRANDMASTER", "I", 2), 2402);
        assert_eq!(league_to_numeric("CHALLENGER", "I", 3), 2403);
        assert_eq!(league_to_numeric("CHALLENGER", "I", 620), 3020);
    }

    #[test]
    #[should_panic]
    fn test_league_to_numeric_invalid_league() {
        league_to_numeric("CHALLENGEJOUR", "I", 1200);
    }

    #[test]
    #[should_panic]
    fn test_league_to_numeric_invalid_division() {
        league_to_numeric("IRON", "V", 0);
    }
}
