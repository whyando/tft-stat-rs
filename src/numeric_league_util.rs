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

pub fn numeric_to_league(mut x: i32) -> (String, String, i32) {
    let tier = match x {
        i32::MIN..=399 => "IRON",
        400..=799 => {
            x -= 400;
            "BRONZE"
        }
        800..=1199 => {
            x -= 800;
            "SILVER"
        }
        1200..=1599 => {
            x -= 1200;
            "GOLD"
        }
        1600..=1999 => {
            x -= 1600;
            "PLATINUM"
        }
        2000..=2399 => {
            x -= 2000;
            "DIAMOND"
        }
        2400..=i32::MAX => {
            x -= 2400;
            "MASTER+"
        }
    };
    let division = match x {
        _ if tier == "MASTER+" => "I",
        i32::MIN..=99 => "IV",
        100..=199 => {
            x -= 100;
            "III"
        }
        200..=299 => {
            x -= 200;
            "II"
        }
        300..=i32::MAX => {
            x -= 300;
            "I"
        }
    };
    (tier.to_string(), division.to_string(), x)
}

pub fn league_to_str(league: &str, rank: &str, lp: i32) -> String {
    format!("{} {} {}LP", league, rank, lp)
}

#[allow(dead_code)]
pub fn elo_to_str(x: i32) -> String {
    let (tier, rank, league_points) = numeric_to_league(x);
    league_to_str(&tier, &rank, league_points)
}

// Given a list of players, return the average elo, in string form
pub fn team_avg_rank_str(ranks: &[(String, String, i32)]) -> String {
    assert!(!ranks.is_empty());
    let mut sum = 0;
    for (tier, rank, league_points) in ranks {
        sum += league_to_numeric(tier, rank, *league_points);
    }
    let x: i32 = sum / (ranks.len() as i32);
    let (mut tier, rank, avg_lp) = numeric_to_league(x);

    if tier == "MASTER+" {
        // Take another average over the 8 players, where
        // CHALLENGER=3, GM=2, MASTER=1. Round to the closest.
        let mut sum = 0;
        for (tier, _, _) in ranks {
            sum += match tier.as_str() {
                "CHALLENGER" => 3,
                "GRANDMASTER" => 2,
                "MASTER" => 1,
                _ => 0,
            }
        }
        tier = if sum < 12 {
            // avg less than 1.5
            "MASTER".to_string()
        } else if sum < 20 {
            // avg less than 2.5
            "GRANDMASTER".to_string()
        } else {
            "CHALLENGER".to_string()
        };
    }

    league_to_str(&tier, &rank, avg_lp)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper function for tests
    fn test_conversions(rank: (&str, &str, i32), elo: i32, elo_string: &str) {
        assert_eq!(league_to_numeric(rank.0, rank.1, rank.2), elo);
        assert_eq!(elo_to_str(elo), elo_string);
    }

    #[test]
    fn test_league_to_numeric() {
        test_conversions(("IRON", "IV", -21), -21, "IRON IV -21LP");
        test_conversions(("IRON", "IV", 0), 0, "IRON IV 0LP");
        test_conversions(("BRONZE", "II", 54), 654, "BRONZE II 54LP");
        test_conversions(("SILVER", "I", 16), 1116, "SILVER I 16LP");
        test_conversions(("GOLD", "IV", 0), 1200, "GOLD IV 0LP");
        test_conversions(("GOLD", "III", 50), 1350, "GOLD III 50LP");
        test_conversions(("GOLD", "III", 100), 1400, "GOLD II 0LP");
        test_conversions(("GOLD", "II", 0), 1400, "GOLD II 0LP");
        test_conversions(("PLATINUM", "III", 31), 1731, "PLATINUM III 31LP");
        test_conversions(("PLATINUM", "III", -32), 1668, "PLATINUM IV 68LP");

        test_conversions(("DIAMOND", "IV", 0), 2000, "DIAMOND IV 0LP");
        test_conversions(("DIAMOND", "III", 0), 2100, "DIAMOND III 0LP");
        test_conversions(("DIAMOND", "II", 0), 2200, "DIAMOND II 0LP");
        test_conversions(("DIAMOND", "I", 0), 2300, "DIAMOND I 0LP");
        test_conversions(("DIAMOND", "I", 99), 2399, "DIAMOND I 99LP");
        test_conversions(("MASTER", "I", 0), 2400, "MASTER+ I 0LP");

        test_conversions(("MASTER", "I", 1), 2401, "MASTER+ I 1LP");
        test_conversions(("GRANDMASTER", "I", 2), 2402, "MASTER+ I 2LP");
        test_conversions(("CHALLENGER", "I", 3), 2403, "MASTER+ I 3LP");
        test_conversions(("CHALLENGER", "I", 620), 3020, "MASTER+ I 620LP");
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

    #[test]
    fn test_team_avg_rank_str() {
        let ret = team_avg_rank_str(&vec![
            ("CHALLENGER".to_string(), "I".to_string(), 1144),
            ("CHALLENGER".to_string(), "I".to_string(), 653),
            ("CHALLENGER".to_string(), "I".to_string(), 625),
            ("GRANDMASTER".to_string(), "I".to_string(), 506),
            ("GRANDMASTER".to_string(), "I".to_string(), 526),
            ("MASTER".to_string(), "I".to_string(), 192),
            ("MASTER".to_string(), "I".to_string(), 0),
            ("DIAMOND".to_string(), "II".to_string(), 0),
        ]);
        assert_eq!(ret, "GRANDMASTER I 430LP");

        let ret = team_avg_rank_str(&vec![
            ("GRANDMASTER".to_string(), "I".to_string(), 270),
            ("MASTER".to_string(), "I".to_string(), 260),
            ("MASTER".to_string(), "I".to_string(), 250),
            ("GRANDMASTER".to_string(), "I".to_string(), 240),
            ("MASTER".to_string(), "I".to_string(), 230),
            ("MASTER".to_string(), "I".to_string(), 220),
            ("MASTER".to_string(), "I".to_string(), 210),
            ("MASTER".to_string(), "I".to_string(), 200),
        ]);
        assert_eq!(ret, "MASTER I 235LP");

        let ret = team_avg_rank_str(&vec![
            ("CHALLENGER".to_string(), "I".to_string(), 570),
            ("CHALLENGER".to_string(), "I".to_string(), 560),
            ("CHALLENGER".to_string(), "I".to_string(), 550),
            ("CHALLENGER".to_string(), "I".to_string(), 540),
            ("GRANDMASTER".to_string(), "I".to_string(), 530),
            ("GRANDMASTER".to_string(), "I".to_string(), 520),
            ("GRANDMASTER".to_string(), "I".to_string(), 510),
            ("GRANDMASTER".to_string(), "I".to_string(), 500),
        ]);
        assert_eq!(ret, "CHALLENGER I 535LP");
    }
}
