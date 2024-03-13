CREATE TABLE football.clubs (
  "club_id" serial PRIMARY KEY,
  "club_name" varchar,
  "club_stadion" varchar
);

CREATE TABLE football.match_stats (
  "match_id" serial PRIMARY KEY,
  "league_id" integer,
  "home_id" integer,
  "away_id" integer,
  "who_win" varchar,
  "home_goals_ht" integer,
  "home_goals_ft" integer,
  "home_shots_on_goal" integer,
  "home_shots_off_goal" integer,
  "home_total_shots" integer,
  "home_blocked_shots" integer,
  "home_shots_insidebox" integer,
  "home_shots_outsidebox" integer,
  "home_fouls" integer,
  "home_corner_kick" integer,
  "home_offsides" integer,
  "home_ball_possession" integer,
  "home_yellow_cards" integer,
  "home_red_cards" integer,
  "home_goalkeeper_saves" integer,
  "home_total_passes" integer,
  "home_passes_accurate" integer,
  "home_passes_percent" float,
  "away_goals_ht" integer,
  "away_goals_ft" integer,
  "away_shots_on_goal" integer,
  "away_shots_off_goal" integer,
  "away_total_shots" integer,
  "away_blocked_shots" integer,
  "away_shots_insidebox" integer,
  "away_shots_outsidebox" integer,
  "away_fouls" integer,
  "away_corner_kick" integer,
  "away_offsides" integer,
  "away_ball_possession" integer,
  "away_yellow_cards" integer,
  "away_red_cards" integer,
  "away_goalkeeper_saves" integer,
  "away_total_passes" integer,
  "away_passes_accurate" integer,
  "away_passes_percent" float,
  "matchday" timestamp
);

CREATE TABLE football.league (
  "league_id" serial PRIMARY KEY,
  "league_name" varchar,
  "league_country" varchar
);

COMMENT ON COLUMN "match_stats"."who_win" IS 'possibilites: home/away/draw';

ALTER TABLE "match_stats" ADD FOREIGN KEY ("home_id") REFERENCES "clubs" ("club_id");

ALTER TABLE "match_stats" ADD FOREIGN KEY ("away_id") REFERENCES "clubs" ("club_id");

ALTER TABLE "match_stats" ADD FOREIGN KEY ("league_id") REFERENCES "league" ("league_id");
