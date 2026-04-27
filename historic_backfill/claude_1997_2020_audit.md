# darko_1997_2020.parquet vs tpdev_box_new.parq audit

Comparison date: 2026-03-16

## Files compared

- **NEW**: `darko_1997_2020.parquet` (685,882 rows x 188 columns, seasons 1997-2020)
- **OLD**: `tpdev_box_new.parq` filtered to seasons <= 2020 (809,050 rows x 206 columns)

## Row count difference

The 123K row gap is structural, not data loss:

- **212,699 rows only in OLD**: all DNP/inactive stubs (`Source=NbaDotComPbpJson`, 0 minutes, 0 stats). The new pipeline does not emit these.
- **~55K rows only in NEW**: mostly team-stat aggregate rows (`NbaDotComID=0`, 2 per game) plus full 1997 season player data. The old file only has 126 rows for 1997.
- **596,351 matched player-game rows** on `Game_SingleGame` + `NbaDotComID`.

## Raw counting stats: nearly identical

For the 596K matched rows, core box-score stats differ on less than 0.15% of rows:

| Stat | Rows Different | % | Max Diff | Primary source of diffs |
|------|---------------|---|----------|------------------------|
| PTS | 42 | 0.01% | 20 | Game 29600070 (Marciulionis fix) |
| AST | 215 | 0.04% | 14 | Game 29600070 + 2017 cluster |
| FGM | 28 | 0.00% | 8 | Game 29600070 |
| FGA | 183 | 0.03% | 16 | Game 29600070 + 2017 |
| FTM | 26 | 0.00% | 4 | Game 29600070 |
| FTA | 18 | 0.00% | 6 | Game 29600070 |
| 3PM | 11 | 0.00% | 4 | Game 29600070 |
| 3PA | 63 | 0.01% | 6 | Game 29600070 + 2017 |
| OREB | 426 | 0.07% | 3 | Spread across seasons |
| DRB | 821 | 0.14% | 6 | Spread, peak in 2017 |
| STL | 437 | 0.07% | 3 | Game 29600070 + 2017 |
| TOV | 181 | 0.03% | 6 | Game 29600070 |
| BLK | 130 | 0.02% | 2 | Game 29600070 + 2017 |
| PF | 27 | 0.00% | 6 | Game 29600070 |

The largest single source of counting-stat differences is game `0029600070` (1997), where the old file has zeros because the cached NBA boxscore omitted Sarunas Marciulionis. The new pipeline fixes this via `boxscore_source_overrides.csv`.

Season 2017 has a secondary cluster of small differences in AST, DRB, STL, and BLK.

## Derived columns with systematic differences

### POSS: one-way vs two-way possession count

**This is a known issue, not an intentional change.**

- **OLD pipeline**: `POSS = POSS_OFF + POSS_DEF` (two-way; total possessions a player was on the floor for across both ends)
- **NEW pipeline**: `POSS = (POSS_OFF + POSS_DEF) / 2` (one-way; average of offensive and defensive possessions)

The ratio is almost exactly 2x on every row. The `/2` is on line 812 of `0c2_build_tpdev_box_stats_version_v9b.py`:

```python
darko["POSS"] = (darko["POSS_OFF"] + darko["POSS_DEF"]) / 2.0
```

The old tpdev convention was two-way (no division). The underlying `POSS_OFF` and `POSS_DEF` columns themselves have small additional noise between the two pipelines, but the dominant factor is the `/2`.

### Pace: follows POSS

Both files use `Pace = 48 * POSS / Minutes / 2`. Because old POSS is ~2x new POSS, old Pace is ~2x new Pace. The correlation between the Pace ratio and POSS ratio is 0.973.

### Per-100-possession rates (_100p columns)

All rate differences are **100% explained by the POSS denominator**. The `_100p` columns use `POSS_OFF` for offensive stats and `POSS_DEF` for defensive stats. Where the raw counting stat matches between files, the rate ratio equals the POSS ratio with zero deviation. There is no independent rate-calculation bug.

### Minutes: mostly 1-second rounding, but 428 rows have period-sized errors

The bulk of the 96K differing rows (16.1%) are benign 1-second rounding differences. However, **428 rows have discrepancies larger than 30 seconds**, and these are systematic bugs in the new pipeline:

| Error magnitude | Count | What it is |
|----------------|-------|------------|
| ~12 minutes | 150 rows | Exactly 1 regulation quarter (720s) |
| ~5 minutes | 168 rows | Exactly 1 overtime period (300s) |
| Other | 110 rows | Combinations/partial periods |

Official boxscore agrees with OLD minutes in **421 of 427** checkable rows. The new pipeline is wrong in these cases.

**Root cause**: The `boxscore_source_loader` that improved Plus_Minus for 99%+ of rows also introduced a period-starter carry-forward bug via `_fill_missing_starters_from_previous_period_end()` in `start_of_period.py`. When this carry-forward is wrong (player subbed out but not detected), they get credited for an entire extra period of seconds. Season 2020 is disproportionately affected (157 of 428 outliers).

These inflated minutes cascade into inflated Plus_Minus — every game where OLD PM beats NEW PM vs the official boxscore is driven by one of these minutes outliers. Full detail in `minutes_outlier_report.md`.

The remaining ~95,800 rows with differences <= 0.1 minutes are all exactly +/- 1 second (0.0167 min), likely a boundary-second rounding difference in stint assignment. Both files store minutes as decimal floats.

### Plus_Minus: improved lineup tracking

- 384,620 rows (64.5%) differ
- Differences are small integers: 55% are +/-1, 29% are +/-2, perfectly symmetric in direction
- Consistent across all seasons (60-69% differ per season)

Both pipelines compute +/- from PBP event parsing (pbpstats `PlusMinus` stat key), not by copying the official boxscore value. The difference comes from **lineup tracking quality**: the new pipeline passes a `boxscore_source_loader` into pbpstats (via `cautious_rerun.py` lines 67-83), which gives the period-starter resolution access to official boxscore data. The old pipeline relied on PBP-only inference for who was on the court.

Validation against official NBA boxscore `PLUS_MINUS` (500-game sample from `nba_raw.db`):
- **NEW matches official**: 99.2% of player-rows
- **OLD matches official**: 35.2% of player-rows

Additional evidence that the new +/- is more correct:
- NEW +/- sums to exactly 0 across both teams in every game (28,985/28,985). OLD fails this balance check for 41 games.
- Diffs are player-specific within games (99.8% of team-games have mixed shift values among players), consistent with lineup-assignment differences around scoring events rather than a systematic bias.
- Players with more minutes have higher diff rates (43% for 0-5 min, 69% for 25-35 min), consistent with more opportunities for lineup-tracking divergence.

## Columns only in one file

- **4 only in NEW**: `0_3ft_FGA_UNAST_100p`, `10_17ft_FGM_100p_UNAST`, `18_23ft_FGM_100p_UNAST`, `Player_Code`
- **22 only in OLD**: demographic/derived columns like `Age`, `Height`, `Weight`, `DraftPick`, `MPG`, `PPG`, `PlayerID`, `PlayerSeasonID`, `Team`, `Seasons`, `YearsInLeague`, etc.

## Columns with zero differences

`FLAGRANT`, `Goaltends`, `h_tm_id`, `season`, `v_tm_id`

## Summary

The new pipeline is a faithful replacement of old tpdev for the raw box-score counting layer through 2020. Differences fall into four categories:

1. **Intentional fixes**: game 29600070, rebound splits, override-backed corrections (~0.1% of rows)
2. **Structural choices**: no DNP stubs emitted, team-stat rows included, full 1997 season present
3. **POSS definition mismatch**: one-way vs two-way possession count, cascading into Pace and all _100p rates. Needs resolution.
4. **Plus_Minus improvement**: new pipeline matches official boxscore +/- 99.2% vs 35.2% for old, thanks to boxscore-informed lineup tracking
5. **Minutes**: ~96K rows with benign 1-second rounding, but **428 rows with period-sized errors (12 min / 5 min)** from a starter carry-forward bug in `start_of_period.py`. These cause the ~25 games where OLD PM beats NEW PM. See Appendix B.


---

# Appendix A: Game-Level Differences


- **NEW file**: `darko_1997_2020.parquet` (685882 rows)
- **OLD file**: `tpdev_box_new.parq` (filtered to season<=2020, 809050 rows)
- **Official source**: `nba_raw.db` (790912 player rows)
- **Merged player rows**: 596351 (inner join on Game_SingleGame + NbaDotComID)
- **Excluded**: Game 29600070, plus 0 games with all-zero old PM for a team

---

## Section 1: Biggest Plus_Minus Differences (Top 20 Games)

Excludes games where old data has all-zero PM for a team, and game 29600070.

### Game 21900970 | Season 2020 | 2020-03-11 | CHA vs MIA
Max absolute PM diff in game: **27.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Bismack Biyombo | CHA | 11.2667 | 11.2000 | -7.0 | -7.0 | -7.0 | 0.0 | NEW |
| Caleb Martin | CHA | 35.0000 | 35.0167 | 15.0 | 15.0 | 15.0 | 0.0 | NEW |
| Cody Martin | CHA | 32.4000 | 32.4000 | 8.0 | 9.0 | 8.0 | -1.0 | NEW |
| Cody Zeller | CHA | 34.4000 | 22.4667 | 34.0 | 7.0 | 8.0 | 27.0 | NEITHER |
| Devonte' Graham | CHA | 36.9500 | 36.9333 | 3.0 | 3.0 | 3.0 | 0.0 | NEW |
| Jalen McDaniels | CHA | 28.9333 | 28.9333 | 17.0 | 17.0 | 17.0 | 0.0 | NEW |
| Joe Chealey | CHA | 2.1833 | 2.1833 | -1.0 | -1.0 | -1.0 | 0.0 | NEW |
| Miles Bridges | CHA | 34.7167 | 34.7333 | 10.0 | 9.0 | 10.0 | 1.0 | NEW |
| P.J. Washington | CHA | 36.1500 | 36.1333 | 2.0 | 3.0 | 2.0 | -1.0 | NEW |
| Andre Iguodala | MIA | 16.8667 | 16.8833 | -11.0 | -11.0 | -11.0 | 0.0 | NEW |
| Bam Adebayo | MIA | 34.2333 | 34.2333 | -10.0 | -9.0 | -10.0 | -1.0 | NEW |
| Derrick Jones Jr. | MIA | 29.6500 | 29.6500 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| Duncan Robinson | MIA | 35.0667 | 35.0500 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Goran Dragic | MIA | 26.0833 | 26.0833 | -13.0 | -14.0 | -13.0 | 1.0 | NEW |
| Jae Crowder | MIA | 31.1667 | 31.1667 | -20.0 | -20.0 | -20.0 | 0.0 | NEW |
| Kelly Olynyk | MIA | 5.3500 | 5.3500 | 6.0 | 5.0 | 6.0 | 1.0 | NEW |
| Kendrick Nunn | MIA | 30.9667 | 30.9500 | 8.0 | 9.0 | 8.0 | -1.0 | NEW |
| Solomon Hill | MIA | 23.3167 | 23.3333 | 0.0 | 0.0 | 0.0 | 0.0 | NEW |
| Tyler Herro | MIA | 7.3000 | 7.3000 | -13.0 | -13.0 | -13.0 | 0.0 | NEW |

### Game 21900291 | Season 2020 | 2019-12-01 | TOR vs UTA
Max absolute PM diff in game: **26.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Chris Boucher | TOR | 4.9500 | 4.9500 | 0.0 | 0.0 | 0.0 | 0.0 | NEW |
| Dewan Hernandez | TOR | 1.6833 | 1.6833 | -2.0 | -2.0 | -2.0 | 0.0 | NEW |
| Fred VanVleet | TOR | 32.4500 | 32.4500 | 19.0 | 16.0 | 19.0 | 3.0 | NEW |
| Malcolm Miller | TOR | 4.5167 | 4.5167 | -1.0 | -1.0 | -1.0 | 0.0 | NEW |
| Marc Gasol | TOR | 25.4500 | 25.4500 | 15.0 | 15.0 | 15.0 | 0.0 | NEW |
| Norman Powell | TOR | 32.8167 | 32.8167 | 5.0 | 6.0 | 5.0 | -1.0 | NEW |
| OG Anunoby | TOR | 29.9333 | 29.9333 | 19.0 | 17.0 | 19.0 | 2.0 | NEW |
| Oshae Brissett | TOR | 4.5167 | 4.5167 | -1.0 | -1.0 | -1.0 | 0.0 | NEW |
| Pascal Siakam | TOR | 34.6000 | 34.6000 | 18.0 | 15.0 | 18.0 | 3.0 | NEW |
| Rondae Hollis-Jefferson | TOR | 23.5000 | 23.5000 | 11.0 | 10.0 | 11.0 | 1.0 | NEW |
| Serge Ibaka | TOR | 20.8667 | 20.8667 | 7.0 | 5.0 | 7.0 | 2.0 | NEW |
| Shamorie Ponds | TOR | 1.6833 | 1.6833 | -2.0 | -2.0 | -2.0 | 0.0 | NEW |
| Terence Davis | TOR | 23.0333 | 23.0333 | 12.0 | 12.0 | 12.0 | 0.0 | NEW |
| Bojan Bogdanovic | UTA | 29.5833 | 29.5833 | -21.0 | -18.0 | -21.0 | -3.0 | NEW |
| Danté Exum | UTA | 11.4667 | 11.4667 | 5.0 | 5.0 | 5.0 | 0.0 | NEW |
| Donovan Mitchell | UTA | 30.7500 | 30.7500 | -24.0 | -20.0 | -24.0 | -4.0 | NEW |
| Ed Davis | UTA | 13.0000 | 13.0000 | -1.0 | -2.0 | -1.0 | 1.0 | NEW |
| Emmanuel Mudiay | UTA | 17.0500 | 17.0500 | -11.0 | -12.0 | -11.0 | 1.0 | NEW |
| Georges Niang | UTA | 12.0000 | 12.0000 | 1.0 | 1.0 | 1.0 | 0.0 | NEW |
| Jeff Green | UTA | 14.5167 | 14.5167 | -13.0 | -13.0 | -13.0 | 0.0 | NEW |
| Joe Ingles | UTA | 25.5667 | 25.5667 | -20.0 | -18.0 | -20.0 | -2.0 | NEW |
| Mike Conley | UTA | 25.1000 | 25.1000 | -6.0 | -4.0 | -6.0 | -2.0 | NEW |
| Nigel Williams-Goss | UTA | 6.5667 | 6.5667 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Royce O'Neale | UTA | 19.4000 | 19.4000 | 7.0 | 5.0 | 7.0 | 2.0 | NEW |
| Rudy Gobert | UTA | 40.4333 | 28.4333 | -44.0 | -18.0 | -21.0 | -26.0 | NEITHER |
| Tony Bradley | UTA | 6.5667 | 6.5667 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |

### Game 29700452 | Season 1998 | 1998-01-04 | SEA vs VAN
Max absolute PM diff in game: **23.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Aaron Williams | SEA | 5.3167 | 5.3167 | -6.0 | -6.0 | -6.0 | 0.0 | NEW |
| Dale Ellis | SEA | 25.9000 | 25.9167 | 7.0 | 7.0 | 7.0 | 0.0 | NEW |
| David Wingate | SEA | 24.1833 | 13.0500 | 11.0 | -12.0 | -12.0 | 23.0 | OLD |
| Detlef Schrempf | SEA | 35.8667 | 35.9000 | 27.0 | 24.0 | 27.0 | 3.0 | NEW |
| Eric Snow | SEA | 1.0333 | 1.0333 | -5.0 | -5.0 | -5.0 | 0.0 | NEW |
| Gary Payton | SEA | 31.6667 | 31.6667 | 15.0 | 13.0 | 15.0 | 2.0 | NEW |
| Greg Anthony | SEA | 16.3333 | 16.3333 | -3.0 | -3.0 | -3.0 | 0.0 | NEW |
| Hersey Hawkins | SEA | 34.4000 | 34.4000 | 20.0 | 18.0 | 20.0 | 2.0 | NEW |
| Jim McIlvaine | SEA | 15.5667 | 15.5667 | 5.0 | 3.0 | 5.0 | 2.0 | NEW |
| Sam Perkins | SEA | 20.1000 | 20.1000 | 0.0 | 0.0 | 0.0 | 0.0 | NEW |
| Stephen Howard | SEA | 2.7333 | 2.7333 | -4.0 | -5.0 | -4.0 | 1.0 | NEW |
| Vin Baker | SEA | 38.9000 | 38.9000 | 16.0 | 16.0 | 16.0 | 0.0 | NEW |
| Antonio Daniels | VAN | 30.2500 | 30.2500 | -13.0 | -11.0 | -13.0 | -2.0 | NEW |
| Blue Edwards | VAN | 19.4167 | 19.4167 | 7.0 | 6.0 | 7.0 | 1.0 | NEW |
| Bryant Reeves | VAN | 25.6333 | 25.6333 | -21.0 | -19.0 | -21.0 | -2.0 | NEW |
| George Lynch | VAN | 22.4833 | 22.4833 | 3.0 | 4.0 | 3.0 | -1.0 | NEW |
| Lee Mayberry | VAN | 17.7500 | 17.7500 | 1.0 | 1.0 | 1.0 | 0.0 | NEW |
| Otis Thorpe | VAN | 36.2333 | 36.2333 | -3.0 | -1.0 | -3.0 | -2.0 | NEW |
| Pete Chilcutt | VAN | 23.7667 | 23.7667 | -10.0 | -9.0 | -10.0 | -1.0 | NEW |
| Sam Mack | VAN | 28.6500 | 28.6500 | -17.0 | -15.0 | -17.0 | -2.0 | NEW |
| Shareef Abdur-Rahim | VAN | 35.8167 | 35.8167 | -7.0 | -6.0 | -7.0 | -1.0 | NEW |

### Game 21900282 | Season 2020 | 2019-11-30 | ATL vs HOU
Max absolute PM diff in game: **21.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Alex Len | ATL | 15.3167 | 15.3167 | -8.0 | -9.0 | -8.0 | 1.0 | NEW |
| Allen Crabbe | ATL | 23.9667 | 23.9667 | -12.0 | -15.0 | -12.0 | 3.0 | NEW |
| Bruno Fernando | ATL | 15.4500 | 15.4500 | -7.0 | -7.0 | -7.0 | 0.0 | NEW |
| Chandler Parsons | ATL | 16.3333 | 16.3333 | -16.0 | -16.0 | -16.0 | 0.0 | NEW |
| Damian Jones | ATL | 14.4000 | 14.4000 | -23.0 | -21.0 | -23.0 | -2.0 | NEW |
| De'Andre Hunter | ATL | 29.0500 | 29.0500 | -25.0 | -25.0 | -25.0 | 0.0 | NEW |
| DeAndre' Bembry | ATL | 23.2000 | 23.2000 | -31.0 | -29.0 | -31.0 | -2.0 | NEW |
| Evan Turner | ATL | 15.5833 | 15.5833 | -20.0 | -19.0 | -20.0 | -1.0 | NEW |
| Jabari Parker | ATL | 20.1333 | 20.1333 | -33.0 | -33.0 | -33.0 | 0.0 | NEW |
| Trae Young | ATL | 31.3833 | 31.3833 | -24.0 | -25.0 | -24.0 | 1.0 | NEW |
| Tyrone Wallace | ATL | 16.6167 | 16.6167 | -23.0 | -22.0 | -23.0 | -1.0 | NEW |
| Vince Carter | ATL | 18.5667 | 18.5667 | -13.0 | -14.0 | -13.0 | 1.0 | NEW |
| Austin Rivers | HOU | 31.8167 | 31.8167 | 21.0 | 20.0 | 21.0 | 1.0 | NEW |
| Ben McLemore | HOU | 33.8667 | 33.8667 | 38.0 | 40.0 | 38.0 | -2.0 | NEW |
| Chris Clemons | HOU | 15.3667 | 15.3667 | -3.0 | -3.0 | -3.0 | 0.0 | NEW |
| Gary Clark | HOU | 26.1000 | 24.4167 | 21.0 | 16.0 | 18.0 | 5.0 | NEITHER |
| Isaiah Hartenstein | HOU | 29.4167 | 20.7833 | 36.0 | 15.0 | 15.0 | 21.0 | OLD |
| James Harden | HOU | 30.6833 | 30.6833 | 50.0 | 50.0 | 50.0 | 0.0 | NEW |
| P.J. Tucker | HOU | 31.4167 | 29.7333 | 40.0 | 38.0 | 39.0 | 2.0 | NEITHER |
| Russell Westbrook | HOU | 26.6833 | 26.6833 | 36.0 | 37.0 | 36.0 | -1.0 | NEW |
| Thabo Sefolosha | HOU | 17.0500 | 17.0500 | 2.0 | 3.0 | 2.0 | -1.0 | NEW |
| Tyson Chandler | HOU | 9.6000 | 9.6000 | 19.0 | 19.0 | 19.0 | 0.0 | NEW |

### Game 21900622 | Season 2020 | 2020-01-17 | CLE vs MEM
Max absolute PM diff in game: **19.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Alfonzo McKinnie | CLE | 26.6000 | 26.6000 | 20.0 | 18.0 | 20.0 | 2.0 | NEW |
| Cedi Osman | CLE | 21.4000 | 21.4000 | -24.0 | -20.0 | -24.0 | -4.0 | NEW |
| Collin Sexton | CLE | 37.0500 | 37.0500 | 10.0 | 11.0 | 10.0 | -1.0 | NEW |
| Danté Exum | CLE | 16.0833 | 16.0833 | 0.0 | 1.0 | 0.0 | -1.0 | NEW |
| Darius Garland | CLE | 30.6667 | 30.6667 | -17.0 | -15.0 | -17.0 | -2.0 | NEW |
| John Henson | CLE | 24.6667 | 12.6667 | -9.0 | 10.0 | 10.0 | -19.0 | OLD |
| Kevin Love | CLE | 34.3833 | 34.3833 | -9.0 | -7.0 | -9.0 | -2.0 | NEW |
| Larry Nance Jr. | CLE | 24.0833 | 24.0833 | 19.0 | 19.0 | 19.0 | 0.0 | NEW |
| Matthew Dellavedova | CLE | 12.2000 | 12.2000 | -1.0 | -1.0 | -1.0 | 0.0 | NEW |
| Tristan Thompson | CLE | 24.8667 | 24.8667 | -27.0 | -26.0 | -28.0 | -1.0 | NEITHER |
| Brandon Clarke | MEM | 25.3167 | 25.3167 | 16.0 | 15.0 | 16.0 | 1.0 | NEW |
| De'Anthony Melton | MEM | 19.9333 | 19.9333 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| Dillon Brooks | MEM | 28.4667 | 28.4667 | 9.0 | 6.0 | 9.0 | 3.0 | NEW |
| Grayson Allen | MEM | 16.1500 | 16.1500 | -3.0 | -4.0 | -3.0 | 1.0 | NEW |
| Ja Morant | MEM | 33.2333 | 33.2333 | 2.0 | -3.0 | 2.0 | 5.0 | NEW |
| Jae Crowder | MEM | 30.3167 | 30.3167 | 20.0 | 19.0 | 20.0 | 1.0 | NEW |
| Jaren Jackson Jr. | MEM | 19.5167 | 19.5167 | -4.0 | -5.0 | -4.0 | 1.0 | NEW |
| Jonas Valančiūnas | MEM | 32.4833 | 32.4667 | 16.0 | 15.0 | 16.0 | 1.0 | NEW |
| Kyle Anderson | MEM | 5.3500 | 5.3500 | -9.0 | -9.0 | -9.0 | 0.0 | NEW |
| Solomon Hill | MEM | 14.8667 | 14.8833 | -25.0 | -25.0 | -25.0 | 0.0 | NEW |
| Tyus Jones | MEM | 14.3667 | 14.3667 | 2.0 | 5.0 | 2.0 | -3.0 | NEW |

### Game 21900937 | Season 2020 | 2020-03-06 | DAL vs MEM
Max absolute PM diff in game: **18.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Boban Marjanovic | DAL | 4.1500 | 4.1500 | 4.0 | 4.0 | 4.0 | 0.0 | NEW |
| Courtney Lee | DAL | 29.2667 | 29.2667 | 25.0 | 26.0 | 25.0 | -1.0 | NEW |
| Delon Wright | DAL | 29.2167 | 29.2167 | 14.0 | 12.0 | 14.0 | 2.0 | NEW |
| J.J. Barea | DAL | 27.2000 | 15.2000 | 12.0 | -6.0 | -6.0 | 18.0 | OLD |
| Justin Jackson | DAL | 31.6833 | 31.6833 | 29.0 | 27.0 | 29.0 | 2.0 | NEW |
| Kristaps Porziņģis | DAL | 29.0167 | 29.0167 | 38.0 | 38.0 | 38.0 | 0.0 | NEW |
| Luka Dončić | DAL | 30.0500 | 30.0500 | -6.0 | -5.0 | -6.0 | -1.0 | NEW |
| Maxi Kleber | DAL | 29.9167 | 29.9167 | 8.0 | 7.0 | 8.0 | 1.0 | NEW |
| Michael Kidd-Gilchrist | DAL | 15.4833 | 15.4833 | 7.0 | 10.0 | 7.0 | -3.0 | NEW |
| Seth Curry | DAL | 15.4500 | 15.4500 | 19.0 | 19.0 | 19.0 | 0.0 | NEW |
| Willie Cauley-Stein | DAL | 10.5667 | 10.5667 | -7.0 | -7.0 | -7.0 | 0.0 | NEW |
| Anthony Tolliver | MEM | 21.2667 | 21.2667 | 0.0 | -1.0 | 0.0 | 1.0 | NEW |
| De'Anthony Melton | MEM | 14.8167 | 14.8167 | -28.0 | -27.0 | -28.0 | -1.0 | NEW |
| Dillon Brooks | MEM | 30.1500 | 30.1500 | -24.0 | -24.0 | -24.0 | 0.0 | NEW |
| Gorgui Dieng | MEM | 17.7333 | 17.7333 | -15.0 | -14.0 | -15.0 | -1.0 | NEW |
| Ja Morant | MEM | 32.4167 | 32.4167 | -27.0 | -27.0 | -27.0 | 0.0 | NEW |
| Jarrod Uthoff | MEM | 4.1500 | 4.1500 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| John Konchar | MEM | 16.3833 | 16.3833 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Jonas Valančiūnas | MEM | 28.1667 | 28.1667 | -14.0 | -14.0 | -14.0 | 0.0 | NEW |
| Josh Jackson | MEM | 21.6167 | 21.6167 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Kyle Anderson | MEM | 24.6833 | 24.6833 | -17.0 | -17.0 | -17.0 | 0.0 | NEW |
| Marko Guduric | MEM | 4.1500 | 4.1500 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| Tyus Jones | MEM | 20.3167 | 20.3167 | 0.0 | -1.0 | 0.0 | 1.0 | NEW |
| Yuta Watanabe | MEM | 4.1500 | 4.1500 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |

### Game 41900231 | Season 2020 | 2020-09-03 | DEN vs LAC
Max absolute PM diff in game: **18.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Bol Bol | DEN | 4.4000 | 4.4000 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Gary Harris | DEN | 24.1667 | 24.1667 | -17.0 | -18.0 | -17.0 | 1.0 | NEW |
| Jamal Murray | DEN | 33.3667 | 33.3667 | -18.0 | -17.0 | -18.0 | -1.0 | NEW |
| Jerami Grant | DEN | 26.3000 | 26.3000 | -16.0 | -19.0 | -16.0 | 3.0 | NEW |
| Keita Bates-Diop | DEN | 6.8000 | 6.8000 | 3.0 | 2.0 | 3.0 | 1.0 | NEW |
| Mason Plumlee | DEN | 13.6500 | 13.6500 | -1.0 | 1.0 | -1.0 | -2.0 | NEW |
| Michael Porter Jr. | DEN | 23.3500 | 23.3500 | -6.0 | -5.0 | -6.0 | -1.0 | NEW |
| Monté Morris | DEN | 17.2833 | 17.2833 | -6.0 | -5.0 | -6.0 | -1.0 | NEW |
| Nikola Jokić | DEN | 29.9500 | 29.9500 | -24.0 | -26.0 | -24.0 | 2.0 | NEW |
| PJ Dozier | DEN | 9.5833 | 9.5833 | 5.0 | 5.0 | 5.0 | 0.0 | NEW |
| Paul Millsap | DEN | 23.8500 | 23.8500 | -16.0 | -16.0 | -16.0 | 0.0 | NEW |
| Torrey Craig | DEN | 21.9500 | 21.9500 | -21.0 | -19.0 | -21.0 | -2.0 | NEW |
| Troy Daniels | DEN | 5.3500 | 5.3500 | 0.0 | 0.0 | 0.0 | 0.0 | NEW |
| Ivica Zubac | LAC | 24.4333 | 24.4333 | 18.0 | 19.0 | 18.0 | -1.0 | NEW |
| JaMychal Green | LAC | 21.1333 | 21.1333 | -1.0 | -1.0 | -1.0 | 0.0 | NEW |
| Kawhi Leonard | LAC | 31.9667 | 31.9667 | 20.0 | 21.0 | 20.0 | -1.0 | NEW |
| Landry Shamet | LAC | 21.1667 | 21.1667 | 3.0 | 3.0 | 3.0 | 0.0 | NEW |
| Lou Williams | LAC | 23.8500 | 23.8500 | 16.0 | 15.0 | 16.0 | 1.0 | NEW |
| Marcus Morris Sr. | LAC | 38.8667 | 26.8667 | 42.0 | 24.0 | 24.0 | 18.0 | OLD |
| Montrezl Harrell | LAC | 19.1667 | 19.1667 | 7.0 | 6.0 | 7.0 | 1.0 | NEW |
| Patrick Beverley | LAC | 12.2167 | 12.2167 | 8.0 | 9.0 | 8.0 | -1.0 | NEW |
| Patrick Patterson | LAC | 4.4000 | 4.4000 | -2.0 | -2.0 | -2.0 | 0.0 | NEW |
| Paul George | LAC | 33.0833 | 33.0833 | 23.0 | 23.0 | 23.0 | 0.0 | NEW |
| Reggie Jackson | LAC | 12.9167 | 12.9167 | 3.0 | 2.0 | 3.0 | 1.0 | NEW |
| Rodney McGruder | LAC | 4.4000 | 4.4000 | -2.0 | -2.0 | -2.0 | 0.0 | NEW |
| Terance Mann | LAC | 4.4000 | 4.4000 | -2.0 | -2.0 | -2.0 | 0.0 | NEW |

### Game 21300690 | Season 2014 | 2014-01-31 | ATL vs PHI
Max absolute PM diff in game: **17.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| DeMarre Carroll | ATL | 26.7500 | 26.7500 | 28.0 | 27.0 | 28.0 | 1.0 | NEW |
| Dennis Schröder | ATL | 16.9000 | 16.9000 | -4.0 | -2.0 | -4.0 | -2.0 | NEW |
| Elton Brand | ATL | 22.6333 | 22.6333 | 15.0 | 14.0 | 15.0 | 1.0 | NEW |
| Gustavo Ayon | ATL | 22.8000 | 22.8000 | 10.0 | 10.0 | 10.0 | 0.0 | NEW |
| James Nunnally | ATL | 12.0000 | 12.0000 | -1.0 | -1.0 | -1.0 | 0.0 | NEW |
| Jared Cunningham | ATL | 4.9667 | 4.9667 | 4.0 | 5.0 | 4.0 | -1.0 | NEW |
| Jeff Teague | ATL | 23.8000 | 23.8000 | 15.0 | 16.0 | 15.0 | -1.0 | NEW |
| Kyle Korver | ATL | 26.8000 | 26.8000 | 22.0 | 21.0 | 22.0 | 1.0 | NEW |
| Lou Williams | ATL | 20.7500 | 20.7500 | 8.0 | 8.0 | 8.0 | 0.0 | NEW |
| Mike Scott | ATL | 23.6500 | 23.6500 | 0.0 | -1.0 | 0.0 | 1.0 | NEW |
| Paul Millsap | ATL | 26.9167 | 26.9167 | 27.0 | 27.0 | 27.0 | 0.0 | NEW |
| Shelvin Mack | ATL | 12.0333 | 12.0333 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Dewayne Dedmon | PHI | 27.8000 | 15.8000 | -19.0 | -2.0 | -4.0 | -17.0 | NEITHER |
| Elliot Williams | PHI | 24.2833 | 24.2833 | -11.0 | -10.0 | -11.0 | -1.0 | NEW |
| Evan Turner | PHI | 26.0000 | 26.0000 | -19.0 | -20.0 | -19.0 | 1.0 | NEW |
| Hollis Thompson | PHI | 24.5167 | 24.5167 | -2.0 | -1.0 | -2.0 | -1.0 | NEW |
| James Anderson | PHI | 28.6000 | 28.6000 | -21.0 | -22.0 | -21.0 | 1.0 | NEW |
| Lavoy Allen | PHI | 21.3000 | 21.3000 | -7.0 | -9.0 | -7.0 | 2.0 | NEW |
| Michael Carter-Williams | PHI | 32.7000 | 32.7000 | -16.0 | -16.0 | -16.0 | 0.0 | NEW |
| Spencer Hawes | PHI | 17.7167 | 17.7167 | -19.0 | -19.0 | -19.0 | 0.0 | NEW |
| Thaddeus Young | PHI | 30.3833 | 30.3833 | -20.0 | -20.0 | -20.0 | 0.0 | NEW |
| Tony Wroten | PHI | 18.7000 | 18.7000 | -11.0 | -11.0 | -11.0 | 0.0 | NEW |

### Game 20600100 | Season 2007 | 2006-11-14 | ATL vs MIL
Max absolute PM diff in game: **16.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Cedric Bozeman | ATL | 10.2167 | 10.2167 | -13.0 | -11.0 | -13.0 | -2.0 | NEW |
| Joe Johnson | ATL | 37.8500 | 37.8500 | -10.0 | -10.0 | -10.0 | 0.0 | NEW |
| Josh Childress | ATL | 37.7333 | 37.7333 | 6.0 | 8.0 | 6.0 | -2.0 | NEW |
| Josh Smith | ATL | 31.8333 | 31.8333 | -9.0 | -9.0 | -9.0 | 0.0 | NEW |
| Lorenzen Wright | ATL | 33.0167 | 21.0167 | 27.0 | 11.0 | 11.0 | 16.0 | OLD |
| Matt Freije | ATL | 0.9000 | 0.9167 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| Salim Stoudamire | ATL | 5.0167 | 5.0167 | 11.0 | 8.0 | 11.0 | 3.0 | NEW |
| Shelden Williams | ATL | 29.7833 | 29.7833 | 2.0 | 0.0 | 2.0 | 2.0 | NEW |
| Tyronn Lue | ATL | 38.7333 | 38.7333 | 9.0 | 9.0 | 9.0 | 0.0 | NEW |
| Zaza Pachulia | ATL | 26.9167 | 26.9167 | -13.0 | -12.0 | -13.0 | -1.0 | NEW |
| Andrew Bogut | MIL | 40.4833 | 40.4833 | 5.0 | 4.0 | 5.0 | 1.0 | NEW |
| Brian Skinner | MIL | 13.7333 | 13.7333 | -2.0 | -4.0 | -2.0 | 2.0 | NEW |
| Charlie Bell | MIL | 21.8000 | 21.8000 | -19.0 | -17.0 | -19.0 | -2.0 | NEW |
| Charlie Villanueva | MIL | 14.7000 | 14.7000 | 12.0 | 14.0 | 12.0 | -2.0 | NEW |
| Dan Gadzuric | MIL | 10.6500 | 10.6500 | -12.0 | -11.0 | -12.0 | -1.0 | NEW |
| Ersan Ilyasova | MIL | 13.7833 | 13.7833 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Michael Redd | MIL | 41.9667 | 41.9667 | 0.0 | -3.0 | 0.0 | 3.0 | NEW |
| Mo Williams | MIL | 39.4500 | 39.4500 | 22.0 | 23.0 | 22.0 | -1.0 | NEW |
| Ruben Patterson | MIL | 34.7833 | 34.7833 | 22.0 | 21.0 | 22.0 | 1.0 | NEW |
| Steve Blake | MIL | 8.6500 | 8.6500 | -20.0 | -19.0 | -20.0 | -1.0 | NEW |

### Game 21900297 | Season 2020 | 2019-12-02 | MIL vs NYK
Max absolute PM diff in game: **16.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| D.J. Wilson | MIL | 23.6000 | 23.6000 | 21.0 | 21.0 | 21.0 | 0.0 | NEW |
| Donte DiVincenzo | MIL | 19.4000 | 19.3833 | 11.0 | 7.0 | 11.0 | 4.0 | NEW |
| Dragan Bender | MIL | 13.4167 | 13.4167 | 5.0 | 4.0 | 5.0 | 1.0 | NEW |
| Eric Bledsoe | MIL | 19.6333 | 19.6333 | 25.0 | 23.0 | 25.0 | 2.0 | NEW |
| Ersan Ilyasova | MIL | 14.3500 | 14.3500 | 14.0 | 10.0 | 14.0 | 4.0 | NEW |
| George Hill | MIL | 16.3667 | 16.3667 | 13.0 | 11.0 | 13.0 | 2.0 | NEW |
| Giannis Antetokounmpo | MIL | 21.9667 | 21.9667 | 25.0 | 22.0 | 25.0 | 3.0 | NEW |
| Khris Middleton | MIL | 17.6667 | 17.6667 | 26.0 | 26.0 | 26.0 | 0.0 | NEW |
| Kyle Korver | MIL | 20.8333 | 20.8667 | 14.0 | 14.0 | 14.0 | 0.0 | NEW |
| Pat Connaughton | MIL | 18.4000 | 18.4000 | 10.0 | 9.0 | 10.0 | 1.0 | NEW |
| Robin Lopez | MIL | 22.6667 | 22.6667 | 23.0 | 23.0 | 23.0 | 0.0 | NEW |
| Thanasis Antetokounmpo | MIL | 12.0000 | 12.0000 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Wesley Matthews | MIL | 19.7000 | 19.7000 | 27.0 | 24.0 | 27.0 | 3.0 | NEW |
| Allonzo Trier | NYK | 11.8333 | 11.8333 | -15.0 | -15.0 | -15.0 | 0.0 | NEW |
| Bobby Portis | NYK | 31.3500 | 19.3500 | -17.0 | -1.0 | 1.0 | -16.0 | NEITHER |
| Damyean Dotson | NYK | 28.7500 | 28.7500 | -17.0 | -13.0 | -17.0 | -4.0 | NEW |
| Dennis Smith Jr. | NYK | 25.6500 | 25.6500 | -28.0 | -24.0 | -28.0 | -4.0 | NEW |
| Ignas Brazdeikis | NYK | 18.0167 | 18.0167 | -8.0 | -7.0 | -8.0 | -1.0 | NEW |
| Julius Randle | NYK | 25.3167 | 25.3333 | -31.0 | -26.0 | -31.0 | -5.0 | NEW |
| Kadeem Allen | NYK | 15.5333 | 15.5333 | -5.0 | -5.0 | -5.0 | 0.0 | NEW |
| Kevin Knox II | NYK | 23.1167 | 23.1167 | -37.0 | -35.0 | -37.0 | -2.0 | NEW |
| Mitchell Robinson | NYK | 23.4833 | 23.4667 | -12.0 | -10.0 | -12.0 | -2.0 | NEW |
| RJ Barrett | NYK | 19.9667 | 19.9667 | -25.0 | -24.0 | -25.0 | -1.0 | NEW |
| Taj Gibson | NYK | 20.6500 | 20.6500 | -33.0 | -30.0 | -33.0 | -3.0 | NEW |
| Wayne Ellington | NYK | 8.3333 | 8.3333 | -10.0 | -10.0 | -10.0 | 0.0 | NEW |

### Game 21900415 | Season 2020 | 2019-12-18 | GSW vs POR
Max absolute PM diff in game: **16.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Alec Burks | GSW | 33.3667 | 33.3667 | -9.0 | -12.0 | -9.0 | 3.0 | NEW |
| D'Angelo Russell | GSW | 32.9667 | 32.9667 | -13.0 | -15.0 | -13.0 | 2.0 | NEW |
| Damion Lee | GSW | 24.9000 | 24.9000 | 1.0 | -5.0 | 1.0 | 6.0 | NEW |
| Draymond Green | GSW | 29.4167 | 29.4167 | -8.0 | -10.0 | -8.0 | 2.0 | NEW |
| Eric Paschall | GSW | 17.9333 | 17.9333 | -2.0 | -4.0 | -2.0 | 2.0 | NEW |
| Glenn Robinson III | GSW | 29.9833 | 29.9833 | 0.0 | -5.0 | 0.0 | 5.0 | NEW |
| Jacob Evans | GSW | 11.6833 | 11.6833 | -2.0 | -3.0 | -2.0 | 1.0 | NEW |
| Jordan Poole | GSW | 8.8667 | 8.8667 | -5.0 | -6.0 | -5.0 | 1.0 | NEW |
| Marquese Chriss | GSW | 16.9000 | 16.9000 | -1.0 | -1.0 | -1.0 | 0.0 | NEW |
| Omari Spellman | GSW | 11.3833 | 11.3833 | -4.0 | -7.0 | -4.0 | 3.0 | NEW |
| Willie Cauley-Stein | GSW | 22.6000 | 22.6000 | -7.0 | -12.0 | -7.0 | 5.0 | NEW |
| Anfernee Simons | POR | 19.3500 | 19.2333 | 0.0 | 5.0 | 0.0 | -5.0 | NEW |
| Anthony Tolliver | POR | 17.6833 | 17.6833 | 8.0 | 11.0 | 8.0 | -3.0 | NEW |
| CJ McCollum | POR | 38.3167 | 38.3167 | 8.0 | 12.0 | 8.0 | -4.0 | NEW |
| Carmelo Anthony | POR | 30.3167 | 30.3167 | 2.0 | 5.0 | 2.0 | -3.0 | NEW |
| Damian Lillard | POR | 37.2667 | 37.2667 | 11.0 | 13.0 | 11.0 | -2.0 | NEW |
| Gary Trent Jr. | POR | 20.7167 | 20.7333 | 3.0 | 6.0 | 3.0 | -3.0 | NEW |
| Hassan Whiteside | POR | 44.7500 | 32.8500 | -8.0 | 8.0 | 2.0 | -16.0 | NEITHER |
| Kent Bazemore | POR | 29.1333 | 29.1333 | 10.0 | 14.0 | 10.0 | -4.0 | NEW |
| Skal Labissiere | POR | 14.4667 | 14.4667 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |

### Game 21900597 | Season 2020 | 2020-01-13 | CLE vs LAL
Max absolute PM diff in game: **16.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Alfonzo McKinnie | CLE | 22.3167 | 22.3167 | -12.0 | -11.0 | -12.0 | -1.0 | NEW |
| Brandon Knight | CLE | 12.3000 | 12.3000 | -8.0 | -8.0 | -8.0 | 0.0 | NEW |
| Cedi Osman | CLE | 34.9833 | 34.9833 | -23.0 | -23.0 | -23.0 | 0.0 | NEW |
| Collin Sexton | CLE | 34.6667 | 34.6667 | -26.0 | -26.0 | -26.0 | 0.0 | NEW |
| Danté Exum | CLE | 19.9000 | 19.9000 | -14.0 | -14.0 | -14.0 | 0.0 | NEW |
| Darius Garland | CLE | 29.3500 | 29.3500 | -5.0 | -6.0 | -5.0 | 1.0 | NEW |
| Dean Wade | CLE | 5.4167 | 5.4167 | -9.0 | -8.0 | -9.0 | -1.0 | NEW |
| John Henson | CLE | 9.0167 | 9.0167 | 0.0 | 0.0 | 0.0 | 0.0 | NEW |
| Kevin Love | CLE | 33.0667 | 33.0667 | -19.0 | -20.0 | -19.0 | 1.0 | NEW |
| Tristan Thompson | CLE | 33.5667 | 33.5667 | -20.0 | -21.0 | -20.0 | 1.0 | NEW |
| Tyler Cook | CLE | 5.4167 | 5.4167 | -9.0 | -8.0 | -9.0 | -1.0 | NEW |
| Alex Caruso | LAL | 22.5333 | 22.5333 | 25.0 | 24.0 | 25.0 | 1.0 | NEW |
| Avery Bradley | LAL | 20.9000 | 20.9000 | 11.0 | 11.0 | 11.0 | 0.0 | NEW |
| Danny Green | LAL | 23.3500 | 23.3500 | 14.0 | 14.0 | 14.0 | 0.0 | NEW |
| Dwight Howard | LAL | 36.9000 | 24.9000 | 26.0 | 10.0 | 11.0 | 16.0 | NEITHER |
| JaVale McGee | LAL | 23.1000 | 23.1000 | 18.0 | 19.0 | 18.0 | -1.0 | NEW |
| Jared Dudley | LAL | 16.9667 | 16.9667 | 9.0 | 10.0 | 9.0 | -1.0 | NEW |
| Kentavious Caldwell-Pope | LAL | 26.1500 | 26.1500 | 10.0 | 10.0 | 10.0 | 0.0 | NEW |
| Kyle Kuzma | LAL | 25.7000 | 25.7000 | 10.0 | 11.0 | 10.0 | -1.0 | NEW |
| LeBron James | LAL | 32.7167 | 32.7167 | 19.0 | 19.0 | 19.0 | 0.0 | NEW |
| Quinn Cook | LAL | 20.3667 | 20.3667 | 12.0 | 11.0 | 12.0 | 1.0 | NEW |
| Troy Daniels | LAL | 3.3167 | 3.3167 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |

### Game 21900757 | Season 2020 | 2020-02-05 | BKN vs GSW
Max absolute PM diff in game: **16.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Caris LeVert | BKN | 26.7000 | 26.7000 | 43.0 | 44.0 | 43.0 | -1.0 | NEW |
| Chris Chiozza | BKN | 8.6167 | 8.6167 | 5.0 | 5.0 | 5.0 | 0.0 | NEW |
| DeAndre Jordan | BKN | 33.4500 | 21.4500 | 23.0 | 7.0 | 8.0 | 16.0 | NEITHER |
| Dzanan Musa | BKN | 9.4500 | 9.4500 | 5.0 | 5.0 | 5.0 | 0.0 | NEW |
| Garrett Temple | BKN | 21.3167 | 21.3167 | 14.0 | 13.0 | 14.0 | 1.0 | NEW |
| Jarrett Allen | BKN | 17.9333 | 17.9333 | 28.0 | 29.0 | 28.0 | -1.0 | NEW |
| Jeremiah Martin | BKN | 8.6167 | 8.6167 | 5.0 | 5.0 | 5.0 | 0.0 | NEW |
| Joe Harris | BKN | 22.1500 | 22.1500 | 26.0 | 27.0 | 26.0 | -1.0 | NEW |
| Rodions Kurucs | BKN | 26.5167 | 26.5167 | 15.0 | 14.0 | 15.0 | 1.0 | NEW |
| Spencer Dinwiddie | BKN | 24.1000 | 24.1000 | 20.0 | 20.0 | 20.0 | 0.0 | NEW |
| Taurean Prince | BKN | 20.2833 | 20.2833 | 28.0 | 28.0 | 28.0 | 0.0 | NEW |
| Theo Pinson | BKN | 9.5000 | 9.5000 | 5.0 | 5.0 | 5.0 | 0.0 | NEW |
| Wilson Chandler | BKN | 23.3667 | 23.3667 | 3.0 | 3.0 | 3.0 | 0.0 | NEW |
| D'Angelo Russell | GSW | 32.5333 | 32.5333 | -48.0 | -48.0 | -48.0 | 0.0 | NEW |
| Damion Lee | GSW | 28.9833 | 28.9833 | -32.0 | -32.0 | -32.0 | 0.0 | NEW |
| Draymond Green | GSW | 20.6833 | 20.6833 | -26.0 | -25.0 | -26.0 | -1.0 | NEW |
| Eric Paschall | GSW | 33.4500 | 33.4500 | -24.0 | -23.0 | -24.0 | -1.0 | NEW |
| Jacob Evans | GSW | 23.7167 | 23.7167 | -5.0 | -6.0 | -5.0 | 1.0 | NEW |
| Jordan Poole | GSW | 26.6000 | 26.6000 | -12.0 | -11.0 | -12.0 | -1.0 | NEW |
| Kevon Looney | GSW | 17.8500 | 17.8500 | 3.0 | 2.0 | 3.0 | 1.0 | NEW |
| Marquese Chriss | GSW | 28.4167 | 28.4167 | -42.0 | -41.0 | -42.0 | -1.0 | NEW |
| Omari Spellman | GSW | 27.7667 | 27.7667 | -19.0 | -21.0 | -19.0 | 2.0 | NEW |

### Game 21900866 | Season 2020 | 2020-02-26 | BKN vs WAS
Max absolute PM diff in game: **16.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Caris LeVert | BKN | 34.1500 | 34.1500 | 2.0 | 1.0 | 2.0 | 1.0 | NEW |
| DeAndre Jordan | BKN | 29.6167 | 29.6167 | -1.0 | -2.0 | -1.0 | 1.0 | NEW |
| Garrett Temple | BKN | 33.6167 | 33.6167 | -6.0 | -6.0 | -6.0 | 0.0 | NEW |
| Jarrett Allen | BKN | 18.3833 | 18.3833 | -3.0 | -2.0 | -3.0 | -1.0 | NEW |
| Joe Harris | BKN | 29.1000 | 29.1000 | -11.0 | -10.0 | -11.0 | -1.0 | NEW |
| Rodions Kurucs | BKN | 17.4833 | 17.4833 | -3.0 | -4.0 | -3.0 | 1.0 | NEW |
| Spencer Dinwiddie | BKN | 33.4167 | 33.4167 | -5.0 | -5.0 | -5.0 | 0.0 | NEW |
| Taurean Prince | BKN | 25.3667 | 25.3667 | 2.0 | 3.0 | 2.0 | -1.0 | NEW |
| Timothe Luwawu-Cabarrot | BKN | 18.8667 | 18.8667 | 5.0 | 5.0 | 5.0 | 0.0 | NEW |
| Bradley Beal | WAS | 38.8333 | 38.8333 | 8.0 | 9.0 | 8.0 | -1.0 | NEW |
| Davis Bertans | WAS | 29.6000 | 29.6000 | 3.0 | 3.0 | 3.0 | 0.0 | NEW |
| Ian Mahinmi | WAS | 19.9167 | 19.9167 | -2.0 | -4.0 | -2.0 | 2.0 | NEW |
| Isaac Bonga | WAS | 16.6667 | 16.6667 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Ish Smith | WAS | 23.2167 | 23.2167 | 4.0 | 3.0 | 4.0 | 1.0 | NEW |
| Jerome Robinson | WAS | 19.9833 | 20.0000 | 6.0 | 7.0 | 6.0 | -1.0 | NEW |
| Moritz Wagner | WAS | 13.0500 | 13.0500 | 2.0 | 4.0 | 2.0 | -2.0 | NEW |
| Rui Hachimura | WAS | 26.9833 | 26.9833 | 0.0 | 0.0 | 0.0 | 0.0 | NEW |
| Shabazz Napier | WAS | 24.0333 | 24.0167 | -1.0 | -2.0 | -1.0 | 1.0 | NEW |
| Thomas Bryant | WAS | 14.9000 | 14.9000 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Troy Brown Jr. | WAS | 24.8167 | 12.8167 | -24.0 | -8.0 | -8.0 | -16.0 | OLD |

### Game 21901292 | Season 2020 | 2020-08-10 | OKC vs PHX
Max absolute PM diff in game: **16.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Abdel Nader | OKC | 25.9667 | 25.9667 | -7.0 | -6.0 | -7.0 | -1.0 | NEW |
| Andre Roberson | OKC | 13.0833 | 13.0833 | -9.0 | -5.0 | -9.0 | -4.0 | NEW |
| Chris Paul | OKC | 23.8000 | 23.8000 | -16.0 | -18.0 | -16.0 | 2.0 | NEW |
| Darius Bazley | OKC | 34.6833 | 34.6833 | -19.0 | -20.0 | -19.0 | 1.0 | NEW |
| Deonte Burton | OKC | 32.0833 | 32.0833 | -18.0 | -17.0 | -18.0 | -1.0 | NEW |
| Devon Hall | OKC | 14.0500 | 14.0500 | -2.0 | -1.0 | -2.0 | -1.0 | NEW |
| Hamidou Diallo | OKC | 22.7000 | 22.7000 | -16.0 | -15.0 | -16.0 | -1.0 | NEW |
| Kevin Hervey | OKC | 12.0000 | 12.0000 | -10.0 | -10.0 | -10.0 | 0.0 | NEW |
| Luguentz Dort | OKC | 20.5833 | 20.5833 | -8.0 | -10.0 | -8.0 | 2.0 | NEW |
| Mike Muscala | OKC | 23.9667 | 23.9667 | -10.0 | -10.0 | -10.0 | 0.0 | NEW |
| Terrance Ferguson | OKC | 17.0833 | 17.0833 | -20.0 | -23.0 | -20.0 | 3.0 | NEW |
| Cameron Johnson | PHX | 32.4500 | 32.4500 | 15.0 | 17.0 | 15.0 | -2.0 | NEW |
| Cameron Payne | PHX | 32.8167 | 32.8167 | 22.0 | 21.0 | 22.0 | 1.0 | NEW |
| Cheick Diallo | PHX | 4.5333 | 4.5333 | 3.0 | 3.0 | 3.0 | 0.0 | NEW |
| Dario Šarić | PHX | 34.9167 | 22.9167 | 25.0 | 9.0 | 9.0 | 16.0 | OLD |
| Deandre Ayton | PHX | 17.0833 | 17.0833 | 22.0 | 22.0 | 22.0 | 0.0 | NEW |
| Devin Booker | PHX | 29.2833 | 29.2833 | 15.0 | 20.0 | 15.0 | -5.0 | NEW |
| Frank Kaminsky | PHX | 3.4667 | 3.4667 | -7.0 | -7.0 | -7.0 | 0.0 | NEW |
| Jalen Lecque | PHX | 5.2667 | 5.2667 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Jevon Carter | PHX | 37.0833 | 37.0833 | 26.0 | 25.0 | 26.0 | 1.0 | NEW |
| Mikal Bridges | PHX | 28.9667 | 28.9667 | 23.0 | 19.0 | 23.0 | 4.0 | NEW |
| Ricky Rubio | PHX | 20.8667 | 20.8667 | 3.0 | 2.0 | 3.0 | 1.0 | NEW |
| Ty Jerome | PHX | 5.2667 | 5.2667 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |

### Game 21600270 | Season 2017 | 2016-11-30 | OKC vs WAS
Max absolute PM diff in game: **15.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Andre Roberson | OKC | 30.0167 | 35.0167 | 1.0 | 16.0 | 12.0 | -15.0 | NEITHER |
| Anthony Morrow | OKC | 31.0667 | 31.0500 | 20.0 | 20.0 | 20.0 | 0.0 | NEW |
| Domantas Sabonis | OKC | 18.6167 | 18.6167 | 5.0 | 7.0 | 5.0 | -2.0 | NEW |
| Enes Freedom | OKC | 14.7667 | 14.7667 | -3.0 | -2.0 | -3.0 | -1.0 | NEW |
| Jerami Grant | OKC | 23.6000 | 23.6000 | -3.0 | -1.0 | -3.0 | -2.0 | NEW |
| Joffrey Lauvergne | OKC | 12.2167 | 12.2167 | 7.0 | 7.0 | 7.0 | 0.0 | NEW |
| Russell Westbrook | OKC | 41.1167 | 41.1167 | 4.0 | 7.0 | 4.0 | -3.0 | NEW |
| Semaj Christon | OKC | 15.3167 | 15.3167 | 12.0 | 11.0 | 12.0 | 1.0 | NEW |
| Steven Adams | OKC | 27.8167 | 27.8167 | -5.0 | -6.0 | -5.0 | 1.0 | NEW |
| Victor Oladipo | OKC | 45.4667 | 45.4667 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Bradley Beal | WAS | 41.5000 | 41.5000 | -5.0 | -7.0 | -5.0 | 2.0 | NEW |
| Jason Smith | WAS | 11.3167 | 11.3333 | -15.0 | -14.0 | -15.0 | -1.0 | NEW |
| John Wall | WAS | 43.6833 | 43.6833 | -11.0 | -13.0 | -11.0 | 2.0 | NEW |
| Kelly Oubre Jr. | WAS | 27.9333 | 27.9333 | 5.0 | 2.0 | 5.0 | 3.0 | NEW |
| Marcin Gortat | WAS | 35.7000 | 35.6833 | 0.0 | -1.0 | 0.0 | 1.0 | NEW |
| Marcus Thornton | WAS | 17.1333 | 17.1333 | -3.0 | -5.0 | -3.0 | 2.0 | NEW |
| Markieff Morris | WAS | 36.7000 | 36.7000 | -17.0 | -18.0 | -17.0 | 1.0 | NEW |
| Otto Porter Jr. | WAS | 39.3333 | 39.3333 | 0.0 | 1.0 | 0.0 | -1.0 | NEW |
| Tomas Satoransky | WAS | 11.7000 | 11.7000 | -9.0 | -10.0 | -9.0 | 1.0 | NEW |

### Game 21800925 | Season 2019 | 2019-02-28 | HOU vs MIA
Max absolute PM diff in game: **15.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Austin Rivers | HOU | 30.9167 | 30.9167 | 8.0 | 7.0 | 8.0 | 1.0 | NEW |
| Chris Paul | HOU | 34.1000 | 34.1000 | -6.0 | -5.0 | -6.0 | -1.0 | NEW |
| Clint Capela | HOU | 48.1667 | 36.1667 | -7.0 | 5.0 | 8.0 | -12.0 | NEITHER |
| Gary Clark | HOU | 31.8500 | 31.8500 | 5.0 | 3.0 | 5.0 | 2.0 | NEW |
| Gerald Green | HOU | 33.9500 | 33.9500 | 0.0 | 1.0 | 0.0 | -1.0 | NEW |
| James Harden | HOU | 43.8667 | 43.8667 | 10.0 | 10.0 | 10.0 | 0.0 | NEW |
| Nene | HOU | 7.0000 | 7.0000 | -11.0 | -8.0 | -11.0 | -3.0 | NEW |
| P.J. Tucker | HOU | 22.1500 | 22.1500 | 1.0 | 2.0 | 1.0 | -1.0 | NEW |
| Bam Adebayo | MIA | 29.1000 | 29.1000 | -13.0 | -8.0 | -13.0 | -5.0 | NEW |
| Derrick Jones Jr. | MIA | 20.2667 | 20.2667 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Dion Waiters | MIA | 32.3833 | 32.3833 | -14.0 | -12.0 | -14.0 | -2.0 | NEW |
| Dwyane Wade | MIA | 26.0000 | 26.0000 | -1.0 | 1.0 | -1.0 | -2.0 | NEW |
| Goran Dragic | MIA | 24.9000 | 24.9000 | 15.0 | 11.0 | 15.0 | 4.0 | NEW |
| Josh Richardson | MIA | 37.2667 | 37.2667 | -3.0 | -3.0 | -3.0 | 0.0 | NEW |
| Justise Winslow | MIA | 43.1000 | 31.1000 | 2.0 | -13.0 | -13.0 | 15.0 | OLD |
| Kelly Olynyk | MIA | 32.9333 | 32.9500 | 5.0 | 0.0 | 5.0 | 5.0 | NEW |
| Rodney McGruder | MIA | 6.0500 | 6.0500 | 7.0 | 7.0 | 7.0 | 0.0 | NEW |

### Game 21900836 | Season 2020 | 2020-02-22 | BKN vs CHA
Max absolute PM diff in game: **15.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Caris LeVert | BKN | 29.9667 | 29.9667 | 15.0 | 14.0 | 15.0 | 1.0 | NEW |
| Chris Chiozza | BKN | 3.1500 | 3.1500 | 8.0 | 8.0 | 8.0 | 0.0 | NEW |
| DeAndre Jordan | BKN | 19.9667 | 19.9667 | 8.0 | 7.0 | 8.0 | 1.0 | NEW |
| Garrett Temple | BKN | 26.4333 | 26.4333 | 14.0 | 14.0 | 14.0 | 0.0 | NEW |
| Jarrett Allen | BKN | 24.8833 | 24.8833 | 13.0 | 14.0 | 13.0 | -1.0 | NEW |
| Joe Harris | BKN | 24.5500 | 24.5500 | 10.0 | 10.0 | 10.0 | 0.0 | NEW |
| Rodions Kurucs | BKN | 3.1500 | 3.1500 | 8.0 | 8.0 | 8.0 | 0.0 | NEW |
| Spencer Dinwiddie | BKN | 28.2167 | 28.2167 | 10.0 | 11.0 | 10.0 | -1.0 | NEW |
| Taurean Prince | BKN | 29.6333 | 29.6333 | 21.0 | 22.0 | 21.0 | -1.0 | NEW |
| Theo Pinson | BKN | 4.3833 | 4.3833 | 11.0 | 11.0 | 11.0 | 0.0 | NEW |
| Timothe Luwawu-Cabarrot | BKN | 23.4500 | 23.4500 | 19.0 | 19.0 | 19.0 | 0.0 | NEW |
| Wilson Chandler | BKN | 22.2167 | 22.2167 | 8.0 | 7.0 | 8.0 | 1.0 | NEW |
| Bismack Biyombo | CHA | 21.6333 | 9.6167 | -25.0 | -10.0 | -10.0 | -15.0 | OLD |
| Caleb Martin | CHA | 7.6333 | 7.6333 | -7.0 | -8.0 | -7.0 | 1.0 | NEW |
| Cody Martin | CHA | 17.0667 | 17.0833 | -11.0 | -12.0 | -11.0 | 1.0 | NEW |
| Cody Zeller | CHA | 21.4833 | 21.4833 | -9.0 | -9.0 | -9.0 | 0.0 | NEW |
| Devonte' Graham | CHA | 26.9333 | 26.9333 | -17.0 | -15.0 | -17.0 | -2.0 | NEW |
| Jalen McDaniels | CHA | 22.9667 | 22.9500 | -14.0 | -14.0 | -14.0 | 0.0 | NEW |
| Joe Chealey | CHA | 3.1500 | 3.1500 | -8.0 | -8.0 | -8.0 | 0.0 | NEW |
| Malik Monk | CHA | 26.9167 | 26.9167 | -21.0 | -20.0 | -21.0 | -1.0 | NEW |
| Miles Bridges | CHA | 34.0167 | 34.0167 | -23.0 | -23.0 | -23.0 | 0.0 | NEW |
| P.J. Washington | CHA | 37.2000 | 37.2167 | -12.0 | -12.0 | -12.0 | 0.0 | NEW |
| Terry Rozier | CHA | 33.0000 | 33.0000 | -13.0 | -14.0 | -13.0 | 1.0 | NEW |

### Game 21901233 | Season 2020 | 2020-07-31 | BKN vs ORL
Max absolute PM diff in game: **15.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Caris LeVert | BKN | 27.8167 | 27.8333 | -26.0 | -23.0 | -26.0 | -3.0 | NEW |
| Chris Chiozza | BKN | 17.3833 | 17.3833 | -22.0 | -21.0 | -22.0 | -1.0 | NEW |
| Donta Hall | BKN | 12.4500 | 12.4500 | 18.0 | 16.0 | 18.0 | 2.0 | NEW |
| Dzanan Musa | BKN | 9.6000 | 9.6000 | 17.0 | 17.0 | 17.0 | 0.0 | NEW |
| Garrett Temple | BKN | 23.3500 | 23.3500 | -14.0 | -15.0 | -14.0 | 1.0 | NEW |
| Jarrett Allen | BKN | 26.8833 | 26.8833 | -24.0 | -22.0 | -24.0 | -2.0 | NEW |
| Jeremiah Martin | BKN | 9.5333 | 9.5333 | 17.0 | 17.0 | 17.0 | 0.0 | NEW |
| Joe Harris | BKN | 29.0333 | 29.0333 | -14.0 | -15.0 | -14.0 | 1.0 | NEW |
| Justin Anderson | BKN | 9.6000 | 9.6000 | 17.0 | 17.0 | 17.0 | 0.0 | NEW |
| Lance Thomas | BKN | 17.9167 | 17.9167 | -21.0 | -19.0 | -21.0 | -2.0 | NEW |
| Rodions Kurucs | BKN | 28.2667 | 16.2667 | -23.0 | -8.0 | -5.0 | -15.0 | NEITHER |
| Timothe Luwawu-Cabarrot | BKN | 21.5500 | 21.5500 | 14.0 | 14.0 | 14.0 | 0.0 | NEW |
| Tyler Johnson | BKN | 18.6167 | 18.6167 | -7.0 | -8.0 | -7.0 | 1.0 | NEW |
| Aaron Gordon | ORL | 26.0000 | 26.0000 | 24.0 | 22.0 | 24.0 | 2.0 | NEW |
| D.J. Augustin | ORL | 26.3500 | 26.3500 | 4.0 | 2.0 | 4.0 | 2.0 | NEW |
| Evan Fournier | ORL | 24.7167 | 24.7167 | 26.0 | 25.0 | 26.0 | 1.0 | NEW |
| Gary Clark | ORL | 5.5167 | 5.5167 | -17.0 | -17.0 | -17.0 | 0.0 | NEW |
| James Ennis III | ORL | 24.9667 | 24.9667 | 3.0 | 0.0 | 3.0 | 3.0 | NEW |
| Jonathan Isaac | ORL | 16.4833 | 16.4833 | 3.0 | 5.0 | 3.0 | -2.0 | NEW |
| Khem Birch | ORL | 16.6667 | 16.6667 | 2.0 | 4.0 | 2.0 | -2.0 | NEW |
| Markelle Fultz | ORL | 18.9500 | 18.9500 | -4.0 | -2.0 | -4.0 | -2.0 | NEW |
| Melvin Frazier Jr. | ORL | 4.3333 | 4.3333 | -12.0 | -13.0 | -12.0 | 1.0 | NEW |
| Michael Carter-Williams | ORL | 21.6500 | 21.6500 | 6.0 | 8.0 | 6.0 | -2.0 | NEW |
| Mo Bamba | ORL | 4.3333 | 4.3333 | -12.0 | -13.0 | -12.0 | 1.0 | NEW |
| Nikola Vučević | ORL | 27.0000 | 27.0000 | 20.0 | 19.0 | 20.0 | 1.0 | NEW |
| Terrence Ross | ORL | 23.0333 | 23.0333 | 7.0 | 10.0 | 7.0 | -3.0 | NEW |

### Game 29701102 | Season 1998 | 1998-04-08 | CHH vs PHI
Max absolute PM diff in game: **15.0**

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Anthony Mason | CHH | 39.0167 | 39.0167 | -17.0 | -19.0 | -17.0 | 2.0 | NEW |
| B.J. Armstrong | CHH | 12.5500 | 12.5667 | 6.0 | 4.0 | 6.0 | 2.0 | NEW |
| Bobby Phills | CHH | 35.4333 | 35.4500 | 4.0 | 2.0 | 4.0 | 2.0 | NEW |
| David Wesley | CHH | 33.5667 | 33.5500 | -10.0 | -7.0 | -10.0 | -3.0 | NEW |
| Dell Curry | CHH | 5.9333 | 5.9333 | -9.0 | -12.0 | -9.0 | 3.0 | NEW |
| Donald Royal | CHH | 6.3167 | 6.3333 | -13.0 | -14.0 | -13.0 | 1.0 | NEW |
| Glen Rice | CHH | 36.8667 | 36.8500 | 5.0 | 3.0 | 5.0 | 2.0 | NEW |
| J.R. Reid | CHH | 14.7167 | 14.7167 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Matt Geiger | CHH | 41.9667 | 29.9667 | -27.0 | -12.0 | -12.0 | -15.0 | OLD |
| Vernon Maxwell | CHH | 6.6833 | 6.6833 | -3.0 | -4.0 | -3.0 | 1.0 | NEW |
| Vlade Divac | CHH | 18.9500 | 18.9500 | 7.0 | 7.0 | 7.0 | 0.0 | NEW |
| Aaron McKie | PHI | 29.3833 | 29.4000 | -9.0 | -7.0 | -9.0 | -2.0 | NEW |
| Allen Iverson | PHI | 41.8000 | 41.7833 | 12.0 | 11.0 | 12.0 | 1.0 | NEW |
| Anthony Parker | PHI | 4.3833 | 4.3833 | -1.0 | 1.0 | -1.0 | -2.0 | NEW |
| Derrick Coleman | PHI | 41.5667 | 41.5500 | 9.0 | 8.0 | 9.0 | 1.0 | NEW |
| Eric Snow | PHI | 27.4667 | 27.4667 | 16.0 | 18.0 | 16.0 | -2.0 | NEW |
| Joe Smith | PHI | 19.5333 | 19.5333 | 4.0 | 4.0 | 4.0 | 0.0 | NEW |
| Mark Davis | PHI | 10.9167 | 10.9333 | 13.0 | 13.0 | 13.0 | 0.0 | NEW |
| Scott Williams | PHI | 15.0167 | 15.0333 | 8.0 | 11.0 | 8.0 | -3.0 | NEW |
| Theo Ratliff | PHI | 34.7667 | 34.7667 | 1.0 | 3.0 | 1.0 | -2.0 | NEW |
| Tim Thomas | PHI | 15.1667 | 15.1833 | -13.0 | -12.0 | -13.0 | -1.0 | NEW |

---

## Section 2: Biggest Counting-Stat Differences (Top 15 Games)

Sum of absolute differences across PTS, AST, OREB, DRB, STL, TOV, BLK, PF, FGM, FGA, FTM, FTA, 3PM, 3PA.

### Game 21600375 | Season 2017 | 2016-12-14 | CHA vs WAS
Total counting-stat diff: **16.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Cody Zeller | CHA | BLK | 2.0 | 1.0 | 1 | -1.0 |
| Frank Kaminsky | CHA | STL | 1.0 | 0.0 | 0 | -1.0 |
| Marvin Williams | CHA | OREB | 1.0 | 0.0 | 0 | -1.0 |
| Michael Kidd-Gilchrist | CHA | OREB | 0.0 | 1.0 | 1 | +1.0 |
| Michael Kidd-Gilchrist | CHA | BLK | 1.0 | 2.0 | 2 | +1.0 |
| Ramon Sessions | CHA | STL | 2.0 | 3.0 | 3 | +1.0 |
| Jason Smith | WAS | DRB | 4.0 | 3.0 | 3 | -1.0 |
| John Wall | WAS | STL | 7.0 | 6.0 | 6 | -1.0 |
| Kelly Oubre Jr. | WAS | DRB | 6.0 | 4.0 | 4 | -2.0 |
| Marcin Gortat | WAS | AST | 2.0 | 1.0 | 1 | -1.0 |
| Marcin Gortat | WAS | DRB | 7.0 | 9.0 | 9 | +2.0 |
| Marcin Gortat | WAS | STL | 2.0 | 3.0 | 3 | +1.0 |
| Marcus Thornton | WAS | AST | 0.0 | 1.0 | 1 | +1.0 |
| Trey Burke | WAS | DRB | 0.0 | 1.0 | 1 | +1.0 |


### Game 21600553 | Season 2017 | 2017-01-07 | BOS vs NOP
Total counting-stat diff: **14.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Anthony Davis | NOP | DRB | 12.0 | 13.0 | 13 | +1.0 |
| Anthony Davis | NOP | FGA | 22.0 | 21.0 | 21 | -1.0 |
| Buddy Hield | NOP | AST | 2.0 | 3.0 | 3 | +1.0 |
| Buddy Hield | NOP | DRB | 4.0 | 5.0 | 5 | +1.0 |
| Buddy Hield | NOP | FGA | 8.0 | 9.0 | 9 | +1.0 |
| Dante Cunningham | NOP | DRB | 3.0 | 2.0 | 2 | -1.0 |
| Dante Cunningham | NOP | FGA | 5.0 | 4.0 | 4 | -1.0 |
| Dante Cunningham | NOP | 3PA | 4.0 | 3.0 | 3 | -1.0 |
| Jrue Holiday | NOP | AST | 5.0 | 4.0 | 4 | -1.0 |
| Jrue Holiday | NOP | DRB | 2.0 | 1.0 | 1 | -1.0 |
| Langston Galloway | NOP | FGA | 12.0 | 13.0 | 13 | +1.0 |
| Langston Galloway | NOP | 3PA | 8.0 | 9.0 | 9 | +1.0 |
| Terrence Jones | NOP | BLK | 1.0 | 0.0 | 0 | -1.0 |
| Tyreke Evans | NOP | BLK | 1.0 | 2.0 | 2 | +1.0 |


### Game 21600795 | Season 2017 | 2017-02-09 | ORL vs PHI
Total counting-stat diff: **14.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Dario Šarić | PHI | PTS | 24.0 | 26.0 | 26 | +2.0 |
| Dario Šarić | PHI | FTM | 5.0 | 7.0 | 7 | +2.0 |
| Dario Šarić | PHI | FTA | 6.0 | 8.0 | 8 | +2.0 |
| Ersan Ilyasova | PHI | PTS | 16.0 | 14.0 | 14 | -2.0 |
| Ersan Ilyasova | PHI | FTM | 7.0 | 5.0 | 5 | -2.0 |
| Ersan Ilyasova | PHI | FTA | 8.0 | 6.0 | 6 | -2.0 |
| Nerlens Noel | PHI | STL | 2.0 | 3.0 | 3 | +1.0 |
| Robert Covington | PHI | STL | 3.0 | 2.0 | 2 | -1.0 |


### Game 21600742 | Season 2017 | 2017-02-02 | LAL vs WAS
Total counting-stat diff: **12.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Brandon Ingram | LAL | BLK | 2.0 | 3.0 | 3 | +1.0 |
| D'Angelo Russell | LAL | STL | 2.0 | 1.0 | 1 | -1.0 |
| Lou Williams | LAL | BLK | 1.0 | 0.0 | 0 | -1.0 |
| Tarik Black | LAL | STL | 0.0 | 1.0 | 1 | +1.0 |
| Jason Smith | WAS | DRB | 4.0 | 5.0 | 5 | +1.0 |
| Jason Smith | WAS | FGA | 5.0 | 4.0 | 4 | -1.0 |
| John Wall | WAS | STL | 3.0 | 2.0 | 2 | -1.0 |
| Marcin Gortat | WAS | STL | 1.0 | 2.0 | 2 | +1.0 |
| Markieff Morris | WAS | DRB | 9.0 | 8.0 | 8 | -1.0 |
| Tomas Satoransky | WAS | AST | 1.0 | 2.0 | 2 | +1.0 |
| Trey Burke | WAS | AST | 3.0 | 2.0 | 2 | -1.0 |
| Trey Burke | WAS | FGA | 8.0 | 9.0 | 9 | +1.0 |


### Game 21700454 | Season 2018 | 2017-12-20 | CHA vs TOR
Total counting-stat diff: **12.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Delon Wright | TOR | PTS | 9.0 | 7.0 | 7 | -2.0 |
| Delon Wright | TOR | FTM | 5.0 | 3.0 | 3 | -2.0 |
| Delon Wright | TOR | FTA | 6.0 | 4.0 | 4 | -2.0 |
| Pascal Siakam | TOR | PTS | 12.0 | 14.0 | 14 | +2.0 |
| Pascal Siakam | TOR | FTM | 4.0 | 6.0 | 6 | +2.0 |
| Pascal Siakam | TOR | FTA | 4.0 | 6.0 | 6 | +2.0 |


### Game 21600008 | Season 2017 | 2016-10-26 | CHA vs MIL
Total counting-stat diff: **10.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Kemba Walker | CHA | DRB | 2.0 | 1.0 | 1 | -1.0 |
| Roy Hibbert | CHA | DRB | 5.0 | 6.0 | 6 | +1.0 |
| Greg Monroe | MIL | BLK | 2.0 | 3.0 | 3 | +1.0 |
| Greg Monroe | MIL | FGA | 14.0 | 13.0 | 13 | -1.0 |
| Greg Monroe | MIL | 3PA | 1.0 | 0.0 | 0 | -1.0 |
| Malcolm Brogdon | MIL | DRB | 5.0 | 6.0 | 6 | +1.0 |
| Malcolm Brogdon | MIL | FGA | 8.0 | 9.0 | 9 | +1.0 |
| Malcolm Brogdon | MIL | 3PA | 2.0 | 3.0 | 3 | +1.0 |
| Michael Beasley | MIL | BLK | 1.0 | 0.0 | 0 | -1.0 |
| Mirza Teletovic | MIL | DRB | 3.0 | 2.0 | 2 | -1.0 |


### Game 21600092 | Season 2017 | 2016-11-06 | LAL vs PHX
Total counting-stat diff: **10.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Brandon Ingram | LAL | OREB | 1.0 | 0.0 | 0 | -1.0 |
| Jordan Clarkson | LAL | OREB | 1.0 | 2.0 | 2 | +1.0 |
| Jordan Clarkson | LAL | BLK | 1.0 | 0.0 | 0 | -1.0 |
| Luol Deng | LAL | STL | 0.0 | 1.0 | 1 | +1.0 |
| Tarik Black | LAL | STL | 1.0 | 0.0 | 0 | -1.0 |
| Tarik Black | LAL | BLK | 1.0 | 2.0 | 2 | +1.0 |
| Brandon Knight | PHX | DRB | 1.0 | 0.0 | 0 | -1.0 |
| Devin Booker | PHX | TOV | 3.0 | 2.0 | 2 | -1.0 |
| Eric Bledsoe | PHX | TOV | 5.0 | 6.0 | 6 | +1.0 |
| Leandro Barbosa | PHX | DRB | 0.0 | 1.0 | 1 | +1.0 |


### Game 21600117 | Season 2017 | 2016-11-10 | MIL vs NOP
Total counting-stat diff: **10.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Anthony Davis | NOP | PTS | 32.0 | 30.0 | 30 | -2.0 |
| Anthony Davis | NOP | FGM | 12.0 | 11.0 | 11 | -1.0 |
| Anthony Davis | NOP | FGA | 25.0 | 24.0 | 24 | -1.0 |
| Dante Cunningham | NOP | STL | 3.0 | 2.0 | 2 | -1.0 |
| Omer Asik | NOP | PTS | 8.0 | 10.0 | 10 | +2.0 |
| Omer Asik | NOP | FGM | 3.0 | 4.0 | 4 | +1.0 |
| Omer Asik | NOP | FGA | 4.0 | 5.0 | 5 | +1.0 |
| Solomon Hill | NOP | STL | 1.0 | 2.0 | 2 | +1.0 |


### Game 21600179 | Season 2017 | 2016-11-18 | BOS vs GSW
Total counting-stat diff: **10.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Draymond Green | GSW | BLK | 1.0 | 2.0 | 2 | +1.0 |
| James Michael McAdoo | GSW | AST | 1.0 | 0.0 | 0 | -1.0 |
| James Michael McAdoo | GSW | DRB | 1.0 | 0.0 | 0 | -1.0 |
| Kevin Durant | GSW | STL | 3.0 | 2.0 | 2 | -1.0 |
| Klay Thompson | GSW | DRB | 4.0 | 3.0 | 3 | -1.0 |
| Stephen Curry | GSW | AST | 7.0 | 8.0 | 8 | +1.0 |
| Stephen Curry | GSW | DRB | 2.0 | 3.0 | 3 | +1.0 |
| Stephen Curry | GSW | STL | 3.0 | 4.0 | 4 | +1.0 |
| Stephen Curry | GSW | BLK | 2.0 | 1.0 | 1 | -1.0 |
| Zaza Pachulia | GSW | DRB | 8.0 | 9.0 | 9 | +1.0 |


### Game 21600353 | Season 2017 | 2016-12-10 | BKN vs SAS
Total counting-stat diff: **10.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Sean Kilpatrick | BKN | PTS | 4.0 | 6.0 | 6 | +2.0 |
| Sean Kilpatrick | BKN | FGM | 1.0 | 2.0 | 2 | +1.0 |
| Sean Kilpatrick | BKN | FGA | 6.0 | 7.0 | 7 | +1.0 |
| Spencer Dinwiddie | BKN | PTS | 6.0 | 4.0 | 4 | -2.0 |
| Spencer Dinwiddie | BKN | FGM | 3.0 | 2.0 | 2 | -1.0 |
| Spencer Dinwiddie | BKN | FGA | 4.0 | 3.0 | 3 | -1.0 |
| Kawhi Leonard | SAS | DRB | 5.0 | 6.0 | 6 | +1.0 |
| Pau Gasol | SAS | DRB | 6.0 | 5.0 | 5 | -1.0 |


### Game 21600394 | Season 2017 | 2016-12-16 | MEM vs SAC
Total counting-stat diff: **10.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Andrew Harrison | MEM | DRB | 1.0 | 0.0 | 0 | -1.0 |
| Andrew Harrison | MEM | BLK | 2.0 | 1.0 | 1 | -1.0 |
| Marc Gasol | MEM | DRB | 6.0 | 5.0 | 5 | -1.0 |
| Troy Daniels | MEM | DRB | 1.0 | 2.0 | 2 | +1.0 |
| Troy Daniels | MEM | BLK | 0.0 | 1.0 | 1 | +1.0 |
| Troy Williams | MEM | DRB | 1.0 | 2.0 | 2 | +1.0 |
| Darren Collison | SAC | FGA | 12.0 | 13.0 | 13 | +1.0 |
| Darren Collison | SAC | 3PA | 2.0 | 3.0 | 3 | +1.0 |
| Garrett Temple | SAC | FGA | 11.0 | 10.0 | 10 | -1.0 |
| Garrett Temple | SAC | 3PA | 6.0 | 5.0 | 5 | -1.0 |


### Game 21600404 | Season 2017 | 2016-12-17 | DEN vs NYK
Total counting-stat diff: **10.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Danilo Gallinari | DEN | DRB | 2.0 | 3.0 | 3 | +1.0 |
| Gary Harris | DEN | DRB | 3.0 | 2.0 | 2 | -1.0 |
| Jameer Nelson | DEN | TOV | 2.0 | 3.0 | 3 | +1.0 |
| Jusuf Nurkić | DEN | TOV | 1.0 | 0.0 | 0 | -1.0 |
| Courtney Lee | NYK | OREB | 2.0 | 1.0 | 1 | -1.0 |
| Courtney Lee | NYK | FGA | 12.0 | 11.0 | 11 | -1.0 |
| Joakim Noah | NYK | OREB | 1.0 | 2.0 | 2 | +1.0 |
| Joakim Noah | NYK | FGA | 3.0 | 4.0 | 4 | +1.0 |
| Kristaps Porziņģis | NYK | DRB | 6.0 | 7.0 | 7 | +1.0 |
| Willy Hernangomez | NYK | DRB | 8.0 | 7.0 | 7 | -1.0 |


### Game 21600405 | Season 2017 | 2016-12-17 | GSW vs POR
Total counting-stat diff: **10.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| CJ McCollum | POR | DRB | 2.0 | 1.0 | 1 | -1.0 |
| Maurice Harkless | POR | DRB | 1.0 | 2.0 | 2 | +1.0 |
| Shabazz Napier | POR | FGA | 8.0 | 6.0 | 6 | -2.0 |
| Shabazz Napier | POR | 3PA | 4.0 | 2.0 | 2 | -2.0 |
| Tim Quarterman | POR | FGA | 0.0 | 2.0 | 2 | +2.0 |
| Tim Quarterman | POR | 3PA | 0.0 | 2.0 | 2 | +2.0 |


### Game 20000261 | Season 2001 | 2000-12-06 | MIL vs NJN
Total counting-stat diff: **8.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Scott Williams | MIL | OREB | 2.0 | 3.0 | 3 | +1.0 |
| Scott Williams | MIL | DRB | 5.0 | 4.0 | 4 | -1.0 |
| Tim Thomas | MIL | OREB | 1.0 | 2.0 | 2 | +1.0 |
| Tim Thomas | MIL | DRB | 3.0 | 2.0 | 2 | -1.0 |
| Johnny Newman | NJN | OREB | 1.0 | 2.0 | 2 | +1.0 |
| Johnny Newman | NJN | DRB | 2.0 | 1.0 | 1 | -1.0 |
| Stephen Jackson | NJN | OREB | 0.0 | 1.0 | 1 | +1.0 |
| Stephen Jackson | NJN | DRB | 4.0 | 3.0 | 3 | -1.0 |


### Game 21600169 | Season 2017 | 2016-11-17 | NYK vs WAS
Total counting-stat diff: **8.0**

| Player | Team | Stat | Old | New | Official | Diff |
|--------|------|------|-----|-----|----------|------|
| Brandon Jennings | NYK | AST | 10.0 | 11.0 | 11 | +1.0 |
| Joakim Noah | NYK | DRB | 4.0 | 5.0 | 5 | +1.0 |
| Justin Holiday | NYK | AST | 1.0 | 0.0 | 0 | -1.0 |
| Sasha Vujacic | NYK | DRB | 1.0 | 0.0 | 0 | -1.0 |
| Bradley Beal | WAS | FGA | 11.0 | 10.0 | 10 | -1.0 |
| Bradley Beal | WAS | 3PA | 6.0 | 5.0 | 5 | -1.0 |
| John Wall | WAS | FGA | 14.0 | 15.0 | 15 | +1.0 |
| John Wall | WAS | 3PA | 5.0 | 6.0 | 6 | +1.0 |


---

## Section 3: Minutes Differences Summary

Total rows with Minutes difference > 0.0001: **96283** out of 596351 merged rows.

### Difference value distribution

| Diff Value | Count |
|------------|-------|
| -0.016667 | 50792 |
| 0.016667 | 41816 |
| -0.033333 | 1818 |
| 0.033333 | 1283 |
| -5.000000 | 117 |
| 12.000000 | 100 |
| -0.050000 | 33 |
| 0.050000 | 23 |
| -4.983333 | 14 |
| -12.000000 | 14 |

### Rows with Minutes diff by season

| Season | Rows with diff | Total rows |
|--------|---------------|------------|
| 1997 | 9 | 111 |
| 1998 | 4386 | 25422 |
| 1999 | 2747 | 16260 |
| 2000 | 4376 | 25769 |
| 2001 | 4235 | 25366 |
| 2002 | 4078 | 25273 |
| 2003 | 4000 | 25703 |
| 2004 | 4121 | 25546 |
| 2005 | 4690 | 26599 |
| 2006 | 4892 | 26636 |
| 2007 | 4922 | 26649 |
| 2008 | 4275 | 26648 |
| 2009 | 4212 | 26370 |
| 2010 | 4504 | 26483 |
| 2011 | 4619 | 26768 |
| 2012 | 3700 | 22500 |
| 2013 | 4520 | 27534 |
| 2014 | 4550 | 27519 |
| 2015 | 4508 | 27633 |
| 2016 | 4334 | 27977 |
| 2017 | 3772 | 27873 |
| 2018 | 3787 | 27832 |
| 2019 | 3717 | 27864 |
| 2020 | 3329 | 24016 |

### 5 Example rows

| Player | Game | Season | Min_new | Min_old | Diff (sec) |
|--------|------|--------|---------|---------|------------|
| Sarunas Marciulionis | 29600070 | 1997 | 17.6333 | 0.0000 | 1058.00 |
| Tom Hammonds | 29600070 | 1997 | 25.8000 | 25.8167 | -1.00 |
| Danny Ferry | 29600070 | 1997 | 31.1333 | 31.1500 | -1.00 |
| Tom Gugliotta | 29600071 | 1997 | 39.3000 | 39.2833 | 1.00 |
| Kevin Garnett | 29600071 | 1997 | 37.5500 | 37.5667 | -1.00 |


---

## Section 4: Games Where OLD Plus_Minus Is Closer to Official Than NEW

Total games where OLD PM has lower total absolute error vs official: **25**

### Summary of top 10

| Game | Total PM err NEW | Total PM err OLD | OLD better by |
|------|-----------------|-----------------|---------------|
| 21900970 | 26.0 | 8.0 | 18.0 |
| 29700438 | 36.0 | 18.0 | 18.0 |
| 20800142 | 20.0 | 12.0 | 8.0 |
| 21900762 | 6.0 | 0.0 | 6.0 |
| 21900282 | 25.0 | 20.0 | 5.0 |
| 41900103 | 19.0 | 14.0 | 5.0 |
| 21900536 | 14.0 | 10.0 | 4.0 |
| 20501110 | 20.0 | 16.0 | 4.0 |
| 20801227 | 14.0 | 10.0 | 4.0 |
| 21900937 | 18.0 | 14.0 | 4.0 |

### Game 21900970 | Season 2020 | 2020-03-11 | CHA vs MIA
Total PM error - NEW: 26.0, OLD: 8.0 (OLD better by 18.0)

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Bismack Biyombo | CHA | 11.2667 | 11.2000 | -7.0 | -7.0 | -7.0 | 0.0 | NEW |
| Caleb Martin | CHA | 35.0000 | 35.0167 | 15.0 | 15.0 | 15.0 | 0.0 | NEW |
| Cody Martin | CHA | 32.4000 | 32.4000 | 8.0 | 9.0 | 8.0 | -1.0 | NEW |
| Cody Zeller | CHA | 34.4000 | 22.4667 | 34.0 | 7.0 | 8.0 | 27.0 | NEITHER |
| Devonte' Graham | CHA | 36.9500 | 36.9333 | 3.0 | 3.0 | 3.0 | 0.0 | NEW |
| Jalen McDaniels | CHA | 28.9333 | 28.9333 | 17.0 | 17.0 | 17.0 | 0.0 | NEW |
| Joe Chealey | CHA | 2.1833 | 2.1833 | -1.0 | -1.0 | -1.0 | 0.0 | NEW |
| Miles Bridges | CHA | 34.7167 | 34.7333 | 10.0 | 9.0 | 10.0 | 1.0 | NEW |
| P.J. Washington | CHA | 36.1500 | 36.1333 | 2.0 | 3.0 | 2.0 | -1.0 | NEW |
| Andre Iguodala | MIA | 16.8667 | 16.8833 | -11.0 | -11.0 | -11.0 | 0.0 | NEW |
| Bam Adebayo | MIA | 34.2333 | 34.2333 | -10.0 | -9.0 | -10.0 | -1.0 | NEW |
| Derrick Jones Jr. | MIA | 29.6500 | 29.6500 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| Duncan Robinson | MIA | 35.0667 | 35.0500 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Goran Dragic | MIA | 26.0833 | 26.0833 | -13.0 | -14.0 | -13.0 | 1.0 | NEW |
| Jae Crowder | MIA | 31.1667 | 31.1667 | -20.0 | -20.0 | -20.0 | 0.0 | NEW |
| Kelly Olynyk | MIA | 5.3500 | 5.3500 | 6.0 | 5.0 | 6.0 | 1.0 | NEW |
| Kendrick Nunn | MIA | 30.9667 | 30.9500 | 8.0 | 9.0 | 8.0 | -1.0 | NEW |
| Solomon Hill | MIA | 23.3167 | 23.3333 | 0.0 | 0.0 | 0.0 | 0.0 | NEW |
| Tyler Herro | MIA | 7.3000 | 7.3000 | -13.0 | -13.0 | -13.0 | 0.0 | NEW |

### Game 29700438 | Season 1998 | 1998-01-02 | PHI vs SEA
Total PM error - NEW: 36.0, OLD: 18.0 (OLD better by 18.0)

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Aaron McKie | PHI | 25.1000 | 25.1000 | -11.0 | -12.0 | -11.0 | 1.0 | NEW |
| Allen Iverson | PHI | 24.1167 | 24.1167 | -14.0 | -15.0 | -14.0 | 1.0 | NEW |
| Anthony Parker | PHI | 8.7833 | 8.6167 | 8.0 | 8.0 | 8.0 | 0.0 | NEW |
| Clar. Weatherspoon | PHI | 16.5000 | 16.5000 | -6.0 | -7.0 | -6.0 | 1.0 | NEW |
| Derrick Coleman | PHI | 34.0667 | 22.2333 | -33.0 | -19.0 | -21.0 | -14.0 | NEITHER |
| Jim Jackson | PHI | 30.8833 | 31.1500 | -20.0 | -19.0 | -20.0 | -1.0 | NEW |
| Kebu Stewart | PHI | 9.8833 | 9.8833 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Mark Davis | PHI | 16.2000 | 16.2000 | -7.0 | -8.0 | -7.0 | 1.0 | NEW |
| Rex Walters | PHI | 7.8333 | 7.8333 | 3.0 | 3.0 | 3.0 | 0.0 | NEW |
| Terry Cummings | PHI | 15.7167 | 15.7167 | -2.0 | -4.0 | -2.0 | 2.0 | NEW |
| Theo Ratliff | PHI | 31.1167 | 31.3500 | -11.0 | -9.0 | -11.0 | -2.0 | NEW |
| Tim Thomas | PHI | 31.8000 | 31.8000 | -10.0 | -9.0 | -10.0 | -1.0 | NEW |
| Aaron Williams | SEA | 20.7000 | 20.7000 | -1.0 | -1.0 | -1.0 | 0.0 | NEW |
| Dale Ellis | SEA | 26.1500 | 26.1500 | 10.0 | 11.0 | 10.0 | -1.0 | NEW |
| David Wingate | SEA | 14.8167 | 14.8333 | -3.0 | -3.0 | -3.0 | 0.0 | NEW |
| Detlef Schrempf | SEA | 31.7500 | 20.8333 | 27.0 | 14.0 | 15.0 | 13.0 | NEITHER |
| Eric Snow | SEA | 8.4500 | 8.4500 | -8.0 | -8.0 | -8.0 | 0.0 | NEW |
| Gary Payton | SEA | 32.8167 | 32.8167 | 19.0 | 19.0 | 19.0 | 0.0 | NEW |
| Greg Anthony | SEA | 15.1833 | 15.1833 | -2.0 | -2.0 | -2.0 | 0.0 | NEW |
| Hersey Hawkins | SEA | 26.8333 | 26.8333 | 20.0 | 20.0 | 20.0 | 0.0 | NEW |
| Jim McIlvaine | SEA | 29.8000 | 17.9167 | 21.0 | 10.0 | 9.0 | 11.0 | NEITHER |
| Sam Perkins | SEA | 20.2000 | 20.2000 | 12.0 | 13.0 | 12.0 | -1.0 | NEW |
| Stephen Howard | SEA | 8.4500 | 8.4500 | -8.0 | -8.0 | -8.0 | 0.0 | NEW |
| Vin Baker | SEA | 28.8500 | 28.8500 | 22.0 | 20.0 | 22.0 | 2.0 | NEW |

### Game 20800142 | Season 2009 | 2008-11-16 | DAL vs NYK
Total PM error - NEW: 20.0, OLD: 12.0 (OLD better by 8.0)

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Antoine Wright | DAL | 13.5333 | 13.5333 | 2.0 | 3.0 | 2.0 | -1.0 | NEW |
| Brandon Bass | DAL | 30.2000 | 30.2000 | 22.0 | 21.0 | 22.0 | 1.0 | NEW |
| Dirk Nowitzki | DAL | 44.1833 | 44.1667 | 13.0 | 13.0 | 13.0 | 0.0 | NEW |
| Erick Dampier | DAL | 4.0833 | 4.0833 | -3.0 | -4.0 | -3.0 | 1.0 | NEW |
| Gerald Green | DAL | 6.5333 | 6.5500 | -8.0 | -9.0 | -8.0 | 1.0 | NEW |
| J.J. Barea | DAL | 13.6333 | 13.6500 | -2.0 | -2.0 | -2.0 | 0.0 | NEW |
| James Singleton | DAL | 26.5500 | 26.5500 | -12.0 | -10.0 | -12.0 | -2.0 | NEW |
| Jason Kidd | DAL | 40.3500 | 40.3500 | 12.0 | 12.0 | 12.0 | 0.0 | NEW |
| Jason Terry | DAL | 40.4333 | 40.4333 | 20.0 | 20.0 | 20.0 | 0.0 | NEW |
| Josh Howard | DAL | 45.5000 | 45.5000 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Chris Duhon | NYK | 47.2667 | 47.2667 | -7.0 | -8.0 | -7.0 | 1.0 | NEW |
| David Lee | NYK | 40.2000 | 36.1667 | -26.0 | -16.0 | -16.0 | -10.0 | OLD |
| Jamal Crawford | NYK | 40.7167 | 40.7167 | -12.0 | -12.0 | -12.0 | 0.0 | NEW |
| Mardy Collins | NYK | 5.5500 | 5.5667 | 2.0 | 1.0 | 2.0 | 1.0 | NEW |
| Nate Robinson | NYK | 29.4000 | 28.4333 | -8.0 | -6.0 | -8.0 | -2.0 | NEW |
| Quentin Richardson | NYK | 36.2333 | 36.2333 | -5.0 | -5.0 | -5.0 | 0.0 | NEW |
| Wilson Chandler | NYK | 23.5667 | 23.5667 | -3.0 | -2.0 | -3.0 | -1.0 | NEW |
| Zach Randolph | NYK | 42.0667 | 47.0667 | 9.0 | -2.0 | -1.0 | 11.0 | NEITHER |

### Game 21900762 | Season 2020 | 2020-02-05 | DEN vs UTA
Total PM error - NEW: 6.0, OLD: 0.0 (OLD better by 6.0)

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Gary Harris | DEN | 38.2833 | 38.2833 | 7.0 | 7.0 | 7.0 | 0.0 | NEW |
| Jamal Murray | DEN | 42.5667 | 42.5667 | 5.0 | 5.0 | 5.0 | 0.0 | NEW |
| Monté Morris | DEN | 41.7000 | 41.6833 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Nikola Jokić | DEN | 39.7667 | 39.7667 | 10.0 | 10.0 | 10.0 | 0.0 | NEW |
| PJ Dozier | DEN | 24.7833 | 24.8000 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Torrey Craig | DEN | 36.3333 | 36.3333 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| Vlatko Čančar | DEN | 16.5667 | 16.5667 | -7.0 | -7.0 | -7.0 | 0.0 | NEW |
| Bojan Bogdanovic | UTA | 34.1333 | 34.1333 | 3.0 | 3.0 | 3.0 | 0.0 | NEW |
| Donovan Mitchell | UTA | 36.1500 | 36.1500 | -6.0 | -6.0 | -6.0 | 0.0 | NEW |
| Georges Niang | UTA | 13.8667 | 13.8667 | -6.0 | -6.0 | -6.0 | 0.0 | NEW |
| Joe Ingles | UTA | 34.7667 | 30.3833 | -4.0 | 2.0 | 2.0 | -6.0 | OLD |
| Jordan Clarkson | UTA | 22.9667 | 22.9667 | -7.0 | -7.0 | -7.0 | 0.0 | NEW |
| Mike Conley | UTA | 34.2333 | 34.2333 | 1.0 | 1.0 | 1.0 | 0.0 | NEW |
| Royce O'Neale | UTA | 27.8833 | 20.2667 | 1.0 | 1.0 | 1.0 | 0.0 | NEW |
| Rudy Gobert | UTA | 38.8667 | 38.8667 | -2.0 | -2.0 | -2.0 | 0.0 | NEW |
| Tony Bradley | UTA | 9.1333 | 9.1333 | -1.0 | -1.0 | -1.0 | 0.0 | NEW |

### Game 21900282 | Season 2020 | 2019-11-30 | ATL vs HOU
Total PM error - NEW: 25.0, OLD: 20.0 (OLD better by 5.0)

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Alex Len | ATL | 15.3167 | 15.3167 | -8.0 | -9.0 | -8.0 | 1.0 | NEW |
| Allen Crabbe | ATL | 23.9667 | 23.9667 | -12.0 | -15.0 | -12.0 | 3.0 | NEW |
| Bruno Fernando | ATL | 15.4500 | 15.4500 | -7.0 | -7.0 | -7.0 | 0.0 | NEW |
| Chandler Parsons | ATL | 16.3333 | 16.3333 | -16.0 | -16.0 | -16.0 | 0.0 | NEW |
| Damian Jones | ATL | 14.4000 | 14.4000 | -23.0 | -21.0 | -23.0 | -2.0 | NEW |
| De'Andre Hunter | ATL | 29.0500 | 29.0500 | -25.0 | -25.0 | -25.0 | 0.0 | NEW |
| DeAndre' Bembry | ATL | 23.2000 | 23.2000 | -31.0 | -29.0 | -31.0 | -2.0 | NEW |
| Evan Turner | ATL | 15.5833 | 15.5833 | -20.0 | -19.0 | -20.0 | -1.0 | NEW |
| Jabari Parker | ATL | 20.1333 | 20.1333 | -33.0 | -33.0 | -33.0 | 0.0 | NEW |
| Trae Young | ATL | 31.3833 | 31.3833 | -24.0 | -25.0 | -24.0 | 1.0 | NEW |
| Tyrone Wallace | ATL | 16.6167 | 16.6167 | -23.0 | -22.0 | -23.0 | -1.0 | NEW |
| Vince Carter | ATL | 18.5667 | 18.5667 | -13.0 | -14.0 | -13.0 | 1.0 | NEW |
| Austin Rivers | HOU | 31.8167 | 31.8167 | 21.0 | 20.0 | 21.0 | 1.0 | NEW |
| Ben McLemore | HOU | 33.8667 | 33.8667 | 38.0 | 40.0 | 38.0 | -2.0 | NEW |
| Chris Clemons | HOU | 15.3667 | 15.3667 | -3.0 | -3.0 | -3.0 | 0.0 | NEW |
| Gary Clark | HOU | 26.1000 | 24.4167 | 21.0 | 16.0 | 18.0 | 5.0 | NEITHER |
| Isaiah Hartenstein | HOU | 29.4167 | 20.7833 | 36.0 | 15.0 | 15.0 | 21.0 | OLD |
| James Harden | HOU | 30.6833 | 30.6833 | 50.0 | 50.0 | 50.0 | 0.0 | NEW |
| P.J. Tucker | HOU | 31.4167 | 29.7333 | 40.0 | 38.0 | 39.0 | 2.0 | NEITHER |
| Russell Westbrook | HOU | 26.6833 | 26.6833 | 36.0 | 37.0 | 36.0 | -1.0 | NEW |
| Thabo Sefolosha | HOU | 17.0500 | 17.0500 | 2.0 | 3.0 | 2.0 | -1.0 | NEW |
| Tyson Chandler | HOU | 9.6000 | 9.6000 | 19.0 | 19.0 | 19.0 | 0.0 | NEW |

### Game 41900103 | Season 2020 | 2020-08-22 | MIL vs ORL
Total PM error - NEW: 19.0, OLD: 14.0 (OLD better by 5.0)

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Brook Lopez | MIL | 29.5667 | 29.5667 | 19.0 | 19.0 | 19.0 | 0.0 | NEW |
| Donte DiVincenzo | MIL | 18.9833 | 18.9833 | -16.0 | -15.0 | -16.0 | -1.0 | NEW |
| Eric Bledsoe | MIL | 25.5667 | 25.5667 | 21.0 | 20.0 | 21.0 | 1.0 | NEW |
| Ersan Ilyasova | MIL | 3.5500 | 3.5500 | -9.0 | -9.0 | -9.0 | 0.0 | NEW |
| George Hill | MIL | 18.8833 | 18.8833 | 2.0 | 3.0 | 2.0 | -1.0 | NEW |
| Giannis Antetokounmpo | MIL | 30.6000 | 30.6000 | 20.0 | 19.0 | 20.0 | 1.0 | NEW |
| Khris Middleton | MIL | 31.2000 | 31.2000 | 30.0 | 28.0 | 30.0 | 2.0 | NEW |
| Kyle Korver | MIL | 18.4500 | 18.4333 | -11.0 | -13.0 | -11.0 | 2.0 | NEW |
| Marvin Williams | MIL | 6.1500 | 6.1500 | 11.0 | 12.0 | 11.0 | -1.0 | NEW |
| Pat Connaughton | MIL | 20.5333 | 20.5333 | 5.0 | 5.0 | 5.0 | 0.0 | NEW |
| Robin Lopez | MIL | 8.3333 | 8.3333 | -13.0 | -12.0 | -13.0 | -1.0 | NEW |
| Sterling Brown | MIL | 3.5500 | 3.5500 | -9.0 | -9.0 | -9.0 | 0.0 | NEW |
| Wesley Matthews | MIL | 24.6333 | 24.6500 | 20.0 | 22.0 | 20.0 | -2.0 | NEW |
| BJ Johnson | ORL | 3.5500 | 3.5500 | 9.0 | 9.0 | 9.0 | 0.0 | NEW |
| D.J. Augustin | ORL | 26.6500 | 26.6500 | -7.0 | -8.0 | -7.0 | 1.0 | NEW |
| Evan Fournier | ORL | 31.5500 | 31.5500 | -28.0 | -28.0 | -28.0 | 0.0 | NEW |
| Gary Clark | ORL | 40.4000 | 34.7500 | -23.0 | -15.0 | -15.0 | -8.0 | OLD |
| James Ennis III | ORL | 15.4167 | 9.0667 | -25.0 | -14.0 | -14.0 | -11.0 | OLD |
| Khem Birch | ORL | 19.8500 | 19.8500 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Markelle Fultz | ORL | 28.0167 | 28.0167 | -19.0 | -18.0 | -19.0 | -1.0 | NEW |
| Nikola Vučević | ORL | 35.7667 | 35.7667 | -20.0 | -20.0 | -20.0 | 0.0 | NEW |
| Terrence Ross | ORL | 23.0000 | 23.0000 | 3.0 | 3.0 | 3.0 | 0.0 | NEW |
| Vic Law | ORL | 3.5500 | 3.5500 | 9.0 | 9.0 | 9.0 | 0.0 | NEW |
| Wes Iwundu | ORL | 24.2500 | 24.2500 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |

### Game 21900536 | Season 2020 | 2020-01-05 | CLE vs MIN
Total PM error - NEW: 14.0, OLD: 10.0 (OLD better by 4.0)

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Alfonzo McKinnie | CLE | 21.0833 | 21.0833 | 20.0 | 20.0 | 20.0 | 0.0 | NEW |
| Ante Zizic | CLE | 24.5833 | 24.5833 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Brandon Knight | CLE | 23.0000 | 23.0000 | 8.0 | 8.0 | 8.0 | 0.0 | NEW |
| Cedi Osman | CLE | 29.7167 | 29.7167 | -35.0 | -35.0 | -35.0 | 0.0 | NEW |
| Collin Sexton | CLE | 35.4667 | 35.4667 | -25.0 | -24.0 | -25.0 | -1.0 | NEW |
| Danté Exum | CLE | 24.1667 | 24.1667 | 7.0 | 5.0 | 7.0 | 2.0 | NEW |
| Darius Garland | CLE | 35.3167 | 35.3167 | -10.0 | -11.0 | -10.0 | 1.0 | NEW |
| John Henson | CLE | 23.4167 | 23.4167 | -17.0 | -17.0 | -17.0 | 0.0 | NEW |
| Kevin Porter Jr. | CLE | 19.4500 | 19.4500 | -15.0 | -14.0 | -15.0 | -1.0 | NEW |
| Matthew Dellavedova | CLE | 3.8000 | 3.8000 | -10.0 | -9.0 | -10.0 | -1.0 | NEW |
| Andrew Wiggins | MIN | 30.9000 | 30.8833 | 19.0 | 19.0 | 19.0 | 0.0 | NEW |
| Gorgui Dieng | MIN | 29.2833 | 29.2667 | 20.0 | 20.0 | 20.0 | 0.0 | NEW |
| Jarrett Culver | MIN | 28.1333 | 28.1333 | 33.0 | 33.0 | 33.0 | 0.0 | NEW |
| Jeff Teague | MIN | 17.1500 | 17.1500 | -13.0 | -13.0 | -13.0 | 0.0 | NEW |
| Jordan Bell | MIN | 0.7667 | 0.7833 | 2.0 | 2.0 | 2.0 | 0.0 | NEW |
| Josh Okogie | MIN | 17.2833 | 17.2833 | 1.0 | 0.0 | 1.0 | 1.0 | NEW |
| Keita Bates-Diop | MIN | 14.1500 | 14.1667 | -14.0 | -13.0 | -14.0 | -1.0 | NEW |
| Kelan Martin | MIN | 19.6833 | 19.7000 | -23.0 | -22.0 | -23.0 | -1.0 | NEW |
| Naz Reid | MIN | 10.8167 | 10.8333 | -11.0 | -11.0 | -11.0 | 0.0 | NEW |
| Noah Vonleh | MIN | 7.9000 | 7.9000 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Robert Covington | MIN | 33.8500 | 33.8333 | 29.0 | 28.0 | 29.0 | 1.0 | NEW |
| Shabazz Napier | MIN | 42.0833 | 30.0667 | 40.0 | 26.0 | 26.0 | 14.0 | OLD |

### Game 20501110 | Season 2006 | 2006-04-05 | ATL vs MIN
Total PM error - NEW: 20.0, OLD: 16.0 (OLD better by 4.0)

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Anthony Grundy | ATL | 4.8500 | 4.8500 | 8.0 | 9.0 | 8.0 | -1.0 | NEW |
| Donta Smith | ATL | 1.0000 | 1.0000 | 2.0 | 1.0 | 2.0 | 1.0 | NEW |
| Esteban Batista | ATL | 14.3667 | 14.3667 | -4.0 | -6.0 | -4.0 | 2.0 | NEW |
| Joe Johnson | ATL | 43.4333 | 43.4333 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| Josh Childress | ATL | 34.3167 | 34.3167 | 9.0 | 9.0 | 9.0 | 0.0 | NEW |
| Josh Smith | ATL | 39.4500 | 39.4500 | -2.0 | 2.0 | -2.0 | -4.0 | NEW |
| Marvin Williams | ATL | 29.1000 | 29.1000 | 5.0 | 4.0 | 5.0 | 1.0 | NEW |
| Royal Ivey | ATL | 10.1833 | 10.1833 | -6.0 | -6.0 | -6.0 | 0.0 | NEW |
| Tyronn Lue | ATL | 31.8000 | 31.8000 | 2.0 | 1.0 | 2.0 | 1.0 | NEW |
| Zaza Pachulia | ATL | 31.5000 | 31.5000 | 0.0 | 0.0 | 0.0 | 0.0 | NEW |
| Eddie Griffin | MIN | 16.1833 | 16.1833 | -6.0 | -6.0 | -6.0 | 0.0 | NEW |
| Justin Reed | MIN | 23.3167 | 18.8000 | -14.0 | -8.0 | -8.0 | -6.0 | OLD |
| Kevin Garnett | MIN | 39.0333 | 31.5500 | 0.0 | 14.0 | 14.0 | -14.0 | OLD |
| Marcus Banks | MIN | 31.9167 | 31.9167 | 0.0 | -2.0 | 0.0 | 2.0 | NEW |
| Mark Blount | MIN | 19.3833 | 19.3833 | -5.0 | -5.0 | -5.0 | 0.0 | NEW |
| Marko Jaric | MIN | 18.8333 | 18.8333 | -4.0 | -2.0 | -4.0 | -2.0 | NEW |
| Rashad McCants | MIN | 30.3833 | 30.3833 | 9.0 | 8.0 | 9.0 | 1.0 | NEW |
| Ricky Davis | MIN | 38.2667 | 38.2667 | 12.0 | 12.0 | 12.0 | 0.0 | NEW |
| Ronald Dupree | MIN | 10.7000 | 10.7000 | 2.0 | 3.0 | 2.0 | -1.0 | NEW |
| Trenton Hassell | MIN | 23.9833 | 24.0000 | -24.0 | -24.0 | -24.0 | 0.0 | NEW |

### Game 20801227 | Season 2009 | 2009-04-15 | NOH vs SAS
Total PM error - NEW: 14.0, OLD: 10.0 (OLD better by 4.0)

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Antonio Daniels | NOH | 5.6833 | 5.6833 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| Chris Paul | NOH | 47.3167 | 47.3167 | -3.0 | -3.0 | -3.0 | 0.0 | NEW |
| David West | NOH | 48.3333 | 48.3333 | -5.0 | -6.0 | -5.0 | 1.0 | NEW |
| Devin Brown | NOH | 5.1167 | 5.1333 | -7.0 | -7.0 | -7.0 | 0.0 | NEW |
| James Posey | NOH | 29.8000 | 29.8000 | -3.0 | -2.0 | -3.0 | -1.0 | NEW |
| Melvin Ely | NOH | 10.7500 | 10.7500 | -6.0 | -6.0 | -6.0 | 0.0 | NEW |
| Peja Stojakovic | NOH | 41.7500 | 41.7333 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| Rasual Butler | NOH | 36.3833 | 41.3833 | -5.0 | -13.0 | -12.0 | 8.0 | NEITHER |
| Sean Marks | NOH | 14.6833 | 14.6833 | 7.0 | 9.0 | 7.0 | -2.0 | NEW |
| Tyson Chandler | NOH | 20.1833 | 20.1833 | 2.0 | 1.0 | 2.0 | 1.0 | NEW |
| Bruce Bowen | SAS | 10.4000 | 10.3833 | -9.0 | -10.0 | -9.0 | 1.0 | NEW |
| Drew Gooden | SAS | 15.2667 | 15.2667 | -10.0 | -10.0 | -10.0 | 0.0 | NEW |
| George Hill | SAS | 3.4667 | 3.4833 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| Ime Udoka | SAS | 37.8667 | 37.8500 | -6.0 | -5.0 | -6.0 | -1.0 | NEW |
| Kurt Thomas | SAS | 17.4000 | 17.4167 | -10.0 | -10.0 | -10.0 | 0.0 | NEW |
| Matt Bonner | SAS | 27.7167 | 27.7167 | 8.0 | 8.0 | 8.0 | 0.0 | NEW |
| Michael Finley | SAS | 39.0167 | 39.0167 | 19.0 | 18.0 | 19.0 | 1.0 | NEW |
| Roger Mason Jr. | SAS | 32.4333 | 37.4333 | 4.0 | 11.0 | 11.0 | -7.0 | OLD |
| Tim Duncan | SAS | 33.7333 | 33.7333 | 21.0 | 21.0 | 21.0 | 0.0 | NEW |
| Tony Parker | SAS | 42.7000 | 42.7000 | 15.0 | 16.0 | 15.0 | -1.0 | NEW |

### Game 21900937 | Season 2020 | 2020-03-06 | DAL vs MEM
Total PM error - NEW: 18.0, OLD: 14.0 (OLD better by 4.0)

| Player | Team | Min_new | Min_old | PM_new | PM_old | PM_official | PM_diff | Matches |
|--------|------|---------|---------|--------|--------|-------------|---------|---------|
| Boban Marjanovic | DAL | 4.1500 | 4.1500 | 4.0 | 4.0 | 4.0 | 0.0 | NEW |
| Courtney Lee | DAL | 29.2667 | 29.2667 | 25.0 | 26.0 | 25.0 | -1.0 | NEW |
| Delon Wright | DAL | 29.2167 | 29.2167 | 14.0 | 12.0 | 14.0 | 2.0 | NEW |
| J.J. Barea | DAL | 27.2000 | 15.2000 | 12.0 | -6.0 | -6.0 | 18.0 | OLD |
| Justin Jackson | DAL | 31.6833 | 31.6833 | 29.0 | 27.0 | 29.0 | 2.0 | NEW |
| Kristaps Porziņģis | DAL | 29.0167 | 29.0167 | 38.0 | 38.0 | 38.0 | 0.0 | NEW |
| Luka Dončić | DAL | 30.0500 | 30.0500 | -6.0 | -5.0 | -6.0 | -1.0 | NEW |
| Maxi Kleber | DAL | 29.9167 | 29.9167 | 8.0 | 7.0 | 8.0 | 1.0 | NEW |
| Michael Kidd-Gilchrist | DAL | 15.4833 | 15.4833 | 7.0 | 10.0 | 7.0 | -3.0 | NEW |
| Seth Curry | DAL | 15.4500 | 15.4500 | 19.0 | 19.0 | 19.0 | 0.0 | NEW |
| Willie Cauley-Stein | DAL | 10.5667 | 10.5667 | -7.0 | -7.0 | -7.0 | 0.0 | NEW |
| Anthony Tolliver | MEM | 21.2667 | 21.2667 | 0.0 | -1.0 | 0.0 | 1.0 | NEW |
| De'Anthony Melton | MEM | 14.8167 | 14.8167 | -28.0 | -27.0 | -28.0 | -1.0 | NEW |
| Dillon Brooks | MEM | 30.1500 | 30.1500 | -24.0 | -24.0 | -24.0 | 0.0 | NEW |
| Gorgui Dieng | MEM | 17.7333 | 17.7333 | -15.0 | -14.0 | -15.0 | -1.0 | NEW |
| Ja Morant | MEM | 32.4167 | 32.4167 | -27.0 | -27.0 | -27.0 | 0.0 | NEW |
| Jarrod Uthoff | MEM | 4.1500 | 4.1500 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| John Konchar | MEM | 16.3833 | 16.3833 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Jonas Valančiūnas | MEM | 28.1667 | 28.1667 | -14.0 | -14.0 | -14.0 | 0.0 | NEW |
| Josh Jackson | MEM | 21.6167 | 21.6167 | 6.0 | 6.0 | 6.0 | 0.0 | NEW |
| Kyle Anderson | MEM | 24.6833 | 24.6833 | -17.0 | -17.0 | -17.0 | 0.0 | NEW |
| Marko Guduric | MEM | 4.1500 | 4.1500 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |
| Tyus Jones | MEM | 20.3167 | 20.3167 | 0.0 | -1.0 | 0.0 | 1.0 | NEW |
| Yuta Watanabe | MEM | 4.1500 | 4.1500 | -4.0 | -4.0 | -4.0 | 0.0 | NEW |



---

# Appendix B: Minutes Outliers (428 rows)


Generated automatically. Total outlier rows (abs diff > 0.5 min): **428**

## Summary

- Rows with official boxscore: 427
- Minutes closer to official - **OLD**: 421
- Minutes closer to official - **NEW**: 6
- PM closer to official - **OLD**: 303
- PM closer to official - **NEW**: 73
- PM closer to official - **TIE**: 51
- Direction: NEW > OLD (inflated) = 269, NEW < OLD = 159

## Season Distribution

| Season | Count |
|--------|-------|
| 1997 | 1 |
| 1998 | 35 |
| 1999 | 13 |
| 2000 | 8 |
| 2001 | 20 |
| 2002 | 14 |
| 2003 | 14 |
| 2004 | 4 |
| 2005 | 14 |
| 2006 | 11 |
| 2007 | 10 |
| 2008 | 16 |
| 2009 | 18 |
| 2010 | 7 |
| 2011 | 6 |
| 2012 | 7 |
| 2013 | 8 |
| 2014 | 7 |
| 2015 | 8 |
| 2016 | 7 |
| 2017 | 6 |
| 2018 | 16 |
| 2019 | 21 |
| 2020 | 157 |

## Diff Distribution

| Range (min) | Count |
|-------------|-------|
| [0, 1) | 19 |
| [1, 2) | 11 |
| [2, 3) | 10 |
| [3, 4) | 10 |
| [4, 5) | 39 |
| [5, 6) | 141 |
| [6, 7) | 8 |
| [7, 8) | 11 |
| [8, 10) | 19 |
| [10, 13) | 159 |
| [13, 20) | 1 |

## Full Outlier List (sorted by abs(Min_diff) desc)

| Game_ID | Season | Date | Player | Team | Min_new | Min_old | Min_official | Min_diff | PM_new | PM_old | PM_official | Min_closer | PM_closer |
|---------|--------|------|--------|------|---------|---------|--------------|----------|--------|--------|-------------|------------|-----------|
| 29600070 | 1997 | 1996-11-10 | Sarunas Marciulionis | 1610612743 | 17.63 | 0.00 | N/A | 17.63 | -20 | 0 | N/A | no_official | no_official |
| 20100810 | 2002 | 2002-02-27 | Brian Grant | 1610612748 | 45.15 | 33.13 | 33.13 | 12.02 | -13 | -11 | -11 | OLD | OLD |
| 21900778 | 2020 | 2020-02-08 | Boban Marjanovic | 1610612742 | 24.92 | 12.90 | 12.90 | 12.02 | 4 | -3 | -4 | OLD | OLD |
| 21900438 | 2020 | 2019-12-21 | Keita Bates-Diop | 1610612750 | 29.72 | 17.70 | 17.70 | 12.02 | -2 | -7 | -8 | OLD | OLD |
| 29701024 | 1998 | 1998-03-29 | Derrick Coleman | 1610612755 | 54.02 | 42.00 | 42.00 | 12.02 | 19 | 9 | 11 | OLD | OLD |
| 21900836 | 2020 | 2020-02-22 | Bismack Biyombo | 1610612766 | 21.63 | 9.62 | 9.62 | 12.02 | -25 | -10 | -10 | OLD | OLD |
| 29900949 | 2000 | 2000-03-19 | Ben Wallace | 1610612753 | 47.73 | 35.72 | 35.72 | 12.02 | -5 | 9 | 7 | OLD | OLD |
| 21900536 | 2020 | 2020-01-05 | Shabazz Napier | 1610612750 | 42.08 | 30.07 | 30.07 | 12.02 | 40 | 26 | 26 | OLD | OLD |
| 21900494 | 2020 | 2019-12-30 | DeAndre Jordan | 1610612751 | 37.38 | 25.37 | 25.37 | 12.02 | 11 | 6 | 6 | OLD | OLD |
| 21600253 | 2017 | 2016-11-28 | Garrett Temple | 1610612758 | 16.32 | 28.33 | 28.33 | -12.02 | 1 | 3 | 3 | OLD | OLD |
| 21900420 | 2020 | 2019-12-20 | Dillon Brooks | 1610612763 | 44.73 | 32.73 | 32.73 | 12.00 | -17 | -5 | -4 | OLD | OLD |
| 21900159 | 2020 | 2019-11-13 | LeBron James | 1610612747 | 38.48 | 26.48 | 26.48 | 12.00 | 13 | 16 | 13 | OLD | NEW |
| 21900661 | 2020 | 2020-01-22 | Gorgui Dieng | 1610612750 | 28.73 | 16.73 | 16.73 | 12.00 | 5 | 1 | 0 | OLD | OLD |
| 21900143 | 2020 | 2019-11-11 | LaMarcus Aldridge | 1610612759 | 44.83 | 32.83 | 32.83 | 12.00 | 7 | -4 | -3 | OLD | OLD |
| 20101048 | 2002 | 2002-03-31 | Jerome Williams | 1610612761 | 28.73 | 16.73 | 16.73 | 12.00 | -2 | -3 | -3 | OLD | OLD |
| 21900832 | 2020 | 2020-02-21 | Jakob Poeltl | 1610612759 | 25.28 | 13.28 | 13.28 | 12.00 | 9 | 3 | 3 | OLD | OLD |
| 21900497 | 2020 | 2019-12-31 | Enes Freedom | 1610612738 | 34.58 | 22.58 | 22.58 | 12.00 | 15 | 14 | 15 | OLD | NEW |
| 21900680 | 2020 | 2020-01-25 | Tony Bradley | 1610612762 | 22.93 | 10.93 | 10.93 | 12.00 | 2 | 3 | 3 | OLD | OLD |
| 21900594 | 2020 | 2020-01-13 | Treveon Graham | 1610612750 | 24.73 | 12.73 | 12.73 | 12.00 | -3 | 6 | 7 | OLD | OLD |
| 21900453 | 2020 | 2019-12-23 | Marvin Bagley III | 1610612758 | 35.03 | 23.03 | 23.03 | 12.00 | -10 | -17 | -18 | OLD | OLD |
| 20400736 | 2005 | 2005-02-11 | Ira Newble | 1610612739 | 6.17 | 18.17 | 18.17 | -12.00 | -6 | -5 | -7 | OLD | NEW |
| 20900869 | 2010 | 2010-02-26 | Metta World Peace | 1610612747 | 24.22 | 36.22 | 36.22 | -12.00 | 6 | 12 | 12 | OLD | OLD |
| 29701082 | 1998 | 1998-04-05 | Matt Maloney | 1610612745 | 18.32 | 30.32 | 30.32 | -12.00 | -9 | -13 | -13 | OLD | OLD |
| 21900473 | 2020 | 2019-12-28 | Rondae Hollis-Jefferson | 1610612761 | 31.33 | 19.33 | 19.33 | 12.00 | 11 | 0 | 0 | OLD | OLD |
| 20500102 | 2006 | 2005-11-15 | James Posey | 1610612748 | 22.92 | 34.92 | 34.92 | -12.00 | 7 | 12 | 10 | OLD | OLD |
| 20100622 | 2002 | 2002-01-27 | Raja Bell | 1610612755 | 24.33 | 12.33 | 12.33 | 12.00 | 9 | 10 | 10 | OLD | OLD |
| 21900225 | 2020 | 2019-11-23 | Frank Kaminsky | 1610612756 | 41.18 | 29.18 | 29.18 | 12.00 | -25 | -18 | -18 | OLD | OLD |
| 20700319 | 2008 | 2007-12-12 | Nate Robinson | 1610612752 | 37.68 | 25.68 | 25.68 | 12.00 | -4 | -2 | -1 | OLD | OLD |
| 21900233 | 2020 | 2019-11-23 | Andre Drummond | 1610612765 | 48.78 | 36.78 | 36.78 | 12.00 | -22 | -18 | -18 | OLD | OLD |
| 41900213 | 2020 | 2020-09-03 | Serge Ibaka | 1610612761 | 33.93 | 21.93 | 21.93 | 12.00 | -9 | -4 | -4 | OLD | OLD |
| 29700045 | 1998 | 1997-11-05 | Clar. Weatherspoon | 1610612755 | 41.43 | 29.43 | 29.43 | 12.00 | -2 | 6 | 6 | OLD | OLD |
| 21800927 | 2019 | 2019-02-28 | Russell Westbrook | 1610612760 | 49.53 | 37.53 | 37.53 | 12.00 | 0 | -3 | -3 | OLD | OLD |
| 21800258 | 2019 | 2018-11-21 | P.J. Tucker | 1610612745 | 37.18 | 25.18 | 25.18 | 12.00 | 8 | 16 | 11 | OLD | NEW |
| 21900291 | 2020 | 2019-12-01 | Rudy Gobert | 1610612762 | 40.43 | 28.43 | 28.43 | 12.00 | -44 | -18 | -21 | OLD | OLD |
| 21900757 | 2020 | 2020-02-05 | DeAndre Jordan | 1610612751 | 33.45 | 21.45 | 21.45 | 12.00 | 23 | 7 | 8 | OLD | OLD |
| 21901238 | 2020 | 2020-07-31 | Boban Marjanovic | 1610612742 | 16.73 | 4.73 | 4.73 | 12.00 | 7 | 8 | 7 | OLD | NEW |
| 21301008 | 2014 | 2014-03-19 | Kirk Hinrich | 1610612741 | 36.45 | 24.45 | 24.45 | 12.00 | 6 | 3 | 4 | OLD | OLD |
| 21900886 | 2020 | 2020-02-28 | JaMychal Green | 1610612746 | 34.70 | 22.70 | 22.70 | 12.00 | 21 | 12 | 12 | OLD | OLD |
| 21400947 | 2015 | 2015-03-10 | Dante Cunningham | 1610612740 | 14.35 | 26.35 | 26.35 | -12.00 | 6 | 9 | 9 | OLD | OLD |
| 21100842 | 2012 | 2012-04-09 | Bismack Biyombo | 1610612766 | 13.85 | 25.85 | 25.85 | -12.00 | -16 | -22 | -23 | OLD | OLD |
| 21901265 | 2020 | 2020-08-05 | Markieff Morris | 1610612747 | 24.60 | 12.60 | 12.60 | 12.00 | -9 | 1 | -1 | OLD | OLD |
| 41900114 | 2020 | 2020-08-23 | Lance Thomas | 1610612751 | 19.10 | 7.10 | 7.10 | 12.00 | -2 | -1 | -3 | OLD | NEW |
| 21900772 | 2020 | 2020-02-07 | Serge Ibaka | 1610612761 | 52.30 | 40.30 | 40.30 | 12.00 | 6 | 8 | 10 | OLD | OLD |
| 21900297 | 2020 | 2019-12-02 | Bobby Portis | 1610612752 | 31.35 | 19.35 | 19.35 | 12.00 | -17 | -1 | 1 | OLD | OLD |
| 21900564 | 2020 | 2020-01-09 | Jayson Tatum | 1610612738 | 47.80 | 35.80 | 35.80 | 12.00 | -13 | -11 | -11 | OLD | OLD |
| 20200261 | 2003 | 2002-12-04 | Doug Christie | 1610612758 | 30.25 | 42.25 | 42.25 | -12.00 | 12 | -1 | 0 | OLD | OLD |
| 20100664 | 2002 | 2002-02-03 | Reggie Miller | 1610612754 | 32.00 | 44.00 | 44.00 | -12.00 | -13 | -18 | -21 | OLD | OLD |
| 21900763 | 2020 | 2020-02-05 | Ivica Zubac | 1610612746 | 33.90 | 21.90 | 21.90 | 12.00 | 9 | 14 | 14 | OLD | OLD |
| 21900763 | 2020 | 2020-02-05 | Chris Silva | 1610612748 | 13.10 | 1.10 | 1.10 | 12.00 | -5 | 0 | 0 | OLD | OLD |
| 21900313 | 2020 | 2019-12-04 | Luka Dončić | 1610612742 | 43.65 | 31.65 | 31.65 | 12.00 | -24 | -13 | -16 | OLD | OLD |
| 21900597 | 2020 | 2020-01-13 | Dwight Howard | 1610612747 | 36.90 | 24.90 | 24.90 | 12.00 | 26 | 10 | 11 | OLD | OLD |
| 21900337 | 2020 | 2019-12-08 | Zach LaVine | 1610612741 | 52.55 | 40.55 | 40.55 | 12.00 | -8 | -7 | -9 | OLD | NEW |
| 21900771 | 2020 | 2020-02-07 | Semi Ojeleye | 1610612738 | 30.90 | 18.90 | 18.90 | 12.00 | -7 | -7 | -9 | OLD | TIE |
| 21900742 | 2020 | 2020-02-03 | Julius Randle | 1610612752 | 50.30 | 38.30 | 38.30 | 12.00 | 1 | -5 | -4 | OLD | OLD |
| 20801223 | 2009 | 2009-04-15 | Mario Chalmers | 1610612748 | 33.75 | 21.75 | 21.75 | 12.00 | 9 | 6 | 6 | OLD | OLD |
| 21900425 | 2020 | 2019-12-20 | Maxi Kleber | 1610612742 | 35.10 | 23.10 | 23.10 | 12.00 | 8 | 11 | 9 | OLD | NEW |
| 21900682 | 2020 | 2020-01-25 | Cristiano Felicio | 1610612741 | 31.95 | 19.95 | 19.95 | 12.00 | -13 | -4 | -7 | OLD | OLD |
| 20100054 | 2002 | 2001-11-06 | Jermaine O'Neal | 1610612754 | 52.45 | 40.45 | 40.45 | 12.00 | 7 | 2 | 2 | OLD | OLD |
| 21300690 | 2014 | 2014-01-31 | Dewayne Dedmon | 1610612755 | 27.80 | 15.80 | 15.80 | 12.00 | -19 | -2 | -4 | OLD | OLD |
| 21900119 | 2020 | 2019-11-08 | Vince Carter | 1610612737 | 28.20 | 16.20 | 16.20 | 12.00 | 7 | -1 | -1 | OLD | OLD |
| 29800394 | 1999 | 1999-03-26 | John Thomas | 1610612761 | 26.80 | 14.80 | 14.80 | 12.00 | 12 | 15 | 14 | OLD | OLD |
| 21901297 | 2020 | 2020-08-11 | Donta Hall | 1610612751 | 42.50 | 30.50 | 30.50 | 12.00 | -1 | 4 | 3 | OLD | OLD |
| 21900651 | 2020 | 2020-01-20 | Tony Bradley | 1610612762 | 28.90 | 16.90 | 16.90 | 12.00 | 11 | 6 | 6 | OLD | OLD |
| 21700615 | 2018 | 2018-01-11 | Pascal Siakam | 1610612761 | 35.50 | 23.50 | 23.50 | 12.00 | 27 | 24 | 24 | OLD | OLD |
| 29800063 | 1999 | 1999-02-12 | A.C. Green | 1610612742 | 28.45 | 16.45 | 16.45 | 12.00 | 0 | -5 | -4 | OLD | OLD |
| 21300100 | 2014 | 2013-11-11 | Brandon Davies | 1610612755 | 26.45 | 14.45 | 14.45 | 12.00 | -15 | -15 | -15 | OLD | TIE |
| 21900917 | 2020 | 2020-03-04 | Romeo Langford | 1610612738 | 25.45 | 13.45 | 13.45 | 12.00 | 5 | 5 | 4 | OLD | TIE |
| 21900001 | 2020 | 2019-10-22 | Brandon Ingram | 1610612740 | 47.10 | 35.10 | 35.10 | 12.00 | -17 | -19 | -19 | OLD | OLD |
| 21900942 | 2020 | 2020-03-07 | Mason Plumlee | 1610612743 | 27.70 | 15.70 | 15.70 | 12.00 | 8 | -1 | -2 | OLD | OLD |
| 21800925 | 2019 | 2019-02-28 | Justise Winslow | 1610612748 | 43.10 | 31.10 | 31.10 | 12.00 | 2 | -13 | -13 | OLD | OLD |
| 21900166 | 2020 | 2019-11-14 | Mason Plumlee | 1610612743 | 31.55 | 19.55 | 19.55 | 12.00 | 3 | 11 | 10 | OLD | OLD |
| 21800276 | 2019 | 2018-11-23 | Juancho Hernangomez | 1610612743 | 20.35 | 32.35 | 32.35 | -12.00 | 15 | 14 | 14 | OLD | OLD |
| 21900179 | 2020 | 2019-11-16 | Thabo Sefolosha | 1610612745 | 25.70 | 13.70 | 13.70 | 12.00 | 8 | 7 | 7 | OLD | OLD |
| 20400905 | 2005 | 2005-03-11 | Melvin Ely | 1610612766 | 34.10 | 22.10 | 22.10 | 12.00 | -12 | -11 | -12 | OLD | NEW |
| 29900635 | 2000 | 2000-02-01 | Samaki Walker | 1610612759 | 13.65 | 1.65 | 1.65 | 12.00 | 4 | -3 | -3 | OLD | OLD |
| 21900239 | 2020 | 2019-11-24 | Patrick Patterson | 1610612746 | 14.75 | 2.75 | 2.75 | 12.00 | 16 | 4 | 4 | OLD | OLD |
| 21900937 | 2020 | 2020-03-06 | J.J. Barea | 1610612742 | 27.20 | 15.20 | 15.20 | 12.00 | 12 | -6 | -6 | OLD | OLD |
| 29900545 | 2000 | 2000-01-19 | Nazr Mohammed | 1610612755 | 16.67 | 4.67 | 4.67 | 12.00 | -18 | -10 | -10 | OLD | OLD |
| 21900922 | 2020 | 2020-03-04 | Kelan Martin | 1610612750 | 20.65 | 8.65 | 8.65 | 12.00 | -3 | 4 | 4 | OLD | OLD |
| 29800688 | 1999 | 1999-05-01 | Adonal Foyle | 1610612744 | 25.15 | 13.15 | 13.15 | 12.00 | -22 | -10 | -11 | OLD | OLD |
| 21900044 | 2020 | 2019-10-28 | Serge Ibaka | 1610612761 | 33.55 | 21.55 | 21.55 | 12.00 | -1 | -9 | -10 | OLD | OLD |
| 29900871 | 2000 | 2000-03-08 | Popeye Jones | 1610612743 | 17.87 | 5.87 | 5.87 | 12.00 | 19 | 12 | 12 | OLD | OLD |
| 21900081 | 2020 | 2019-11-02 | Dario Šarić | 1610612756 | 36.55 | 24.55 | 24.55 | 12.00 | 14 | 18 | 18 | OLD | OLD |
| 21900341 | 2020 | 2019-12-08 | Jaylen Hoard | 1610612757 | 13.47 | 1.47 | 1.47 | 12.00 | -6 | 2 | 2 | OLD | OLD |
| 21900827 | 2020 | 2020-02-21 | Cedi Osman | 1610612739 | 42.05 | 30.05 | 30.05 | 12.00 | 17 | 3 | 6 | OLD | OLD |
| 21900885 | 2020 | 2020-02-28 | Davis Bertans | 1610612764 | 40.30 | 28.30 | 28.30 | 12.00 | -24 | -13 | -13 | OLD | OLD |
| 41900131 | 2020 | 2020-08-18 | Bam Adebayo | 1610612748 | 46.82 | 34.82 | 34.82 | 12.00 | 33 | 25 | 23 | OLD | OLD |
| 21900079 | 2020 | 2019-11-02 | Terrence Ross | 1610612753 | 34.32 | 22.32 | 22.32 | 12.00 | -12 | 0 | -1 | OLD | OLD |
| 21900217 | 2020 | 2019-11-22 | Jarrett Allen | 1610612751 | 37.32 | 25.32 | 25.32 | 12.00 | 12 | 7 | 8 | OLD | OLD |
| 21900292 | 2020 | 2019-12-01 | Troy Brown Jr. | 1610612764 | 34.07 | 22.07 | 22.07 | 12.00 | -1 | -10 | -8 | OLD | OLD |
| 21900560 | 2020 | 2020-01-08 | Jaxson Hayes | 1610612740 | 35.97 | 23.97 | 23.97 | 12.00 | 8 | 6 | 6 | OLD | OLD |
| 21900059 | 2020 | 2019-10-30 | Justin Holiday | 1610612754 | 36.37 | 24.37 | 24.37 | 12.00 | 13 | 12 | 13 | OLD | NEW |
| 29701102 | 1998 | 1998-04-08 | Matt Geiger | 1610612766 | 41.97 | 29.97 | 29.97 | 12.00 | -27 | -12 | -12 | OLD | OLD |
| 20700319 | 2008 | 2007-12-12 | Fred Jones | 1610612752 | 19.93 | 31.93 | 31.93 | -12.00 | 0 | 0 | -3 | OLD | TIE |
| 21900622 | 2020 | 2020-01-17 | John Henson | 1610612739 | 24.67 | 12.67 | 12.67 | 12.00 | -9 | 10 | 10 | OLD | OLD |
| 21900419 | 2020 | 2019-12-19 | Maurice Harkless | 1610612746 | 28.12 | 16.12 | 16.12 | 12.00 | 10 | -3 | -4 | OLD | OLD |
| 20300052 | 2004 | 2003-11-05 | David West | 1610612740 | 27.42 | 15.42 | 15.42 | 12.00 | 9 | 12 | 12 | OLD | OLD |
| 21500512 | 2016 | 2016-01-04 | Gorgui Dieng | 1610612750 | 39.47 | 27.47 | 27.47 | 12.00 | -7 | -3 | -3 | OLD | OLD |
| 21900644 | 2020 | 2020-01-20 | Kelly Olynyk | 1610612748 | 30.32 | 18.32 | 18.32 | 12.00 | -8 | -7 | -5 | OLD | OLD |
| 21700377 | 2018 | 2017-12-09 | Jordan Clarkson | 1610612747 | 38.97 | 26.97 | 26.97 | 12.00 | 17 | 13 | 13 | OLD | OLD |
| 41900225 | 2020 | 2020-09-12 | Alex Caruso | 1610612747 | 35.62 | 23.62 | 23.62 | 12.00 | 11 | -1 | -4 | OLD | OLD |
| 41700175 | 2018 | 2018-04-25 | Joe Ingles | 1610612762 | 29.78 | 41.78 | 41.78 | -12.00 | 8 | 0 | 0 | OLD | OLD |
| 41900231 | 2020 | 2020-09-03 | Marcus Morris Sr. | 1610612746 | 38.87 | 26.87 | 26.87 | 12.00 | 42 | 24 | 24 | OLD | OLD |
| 21900851 | 2020 | 2020-02-24 | Moritz Wagner | 1610612764 | 40.47 | 28.47 | 28.47 | 12.00 | 8 | 11 | 11 | OLD | OLD |
| 29900374 | 2000 | 1999-12-23 | Hubert Davis | 1610612742 | 29.28 | 41.28 | 41.28 | -12.00 | -3 | -8 | -8 | OLD | OLD |
| 20101105 | 2002 | 2002-04-08 | Pat Garrity | 1610612753 | 26.43 | 38.43 | 38.43 | -12.00 | 15 | 18 | 18 | OLD | OLD |
| 21900715 | 2020 | 2020-01-30 | Thomas Bryant | 1610612764 | 35.37 | 23.37 | 23.37 | 12.00 | 19 | 8 | 7 | OLD | OLD |
| 29700525 | 1998 | 1998-01-15 | Toni Kukoc | 1610612741 | 31.07 | 19.07 | 19.07 | 12.00 | -9 | 1 | 2 | OLD | OLD |
| 21900866 | 2020 | 2020-02-26 | Troy Brown Jr. | 1610612764 | 24.82 | 12.82 | 12.82 | 12.00 | -24 | -8 | -8 | OLD | OLD |
| 21801034 | 2019 | 2019-03-16 | Trae Young | 1610612737 | 43.27 | 31.27 | 31.27 | 12.00 | -12 | -17 | -16 | OLD | OLD |
| 21900612 | 2020 | 2020-01-15 | Luka Dončić | 1610612742 | 46.12 | 34.12 | 34.12 | 12.00 | -2 | 10 | 9 | OLD | OLD |
| 29700560 | 1998 | 1998-01-20 | Sam Perkins | 1610612760 | 28.22 | 16.22 | 16.22 | 12.00 | -3 | -1 | -2 | OLD | TIE |
| 21900426 | 2020 | 2019-12-20 | Nerlens Noel | 1610612760 | 32.17 | 20.17 | 20.17 | 12.00 | 13 | 8 | 10 | OLD | OLD |
| 20200060 | 2003 | 2002-11-06 | Brian Skinner | 1610612755 | 35.27 | 23.27 | 23.27 | 12.00 | 3 | 10 | 9 | OLD | OLD |
| 21901233 | 2020 | 2020-07-31 | Rodions Kurucs | 1610612751 | 28.27 | 16.27 | 16.27 | 12.00 | -23 | -8 | -5 | OLD | OLD |
| 20000964 | 2001 | 2001-03-18 | Brent Barry | 1610612760 | 18.98 | 30.98 | 30.98 | -12.00 | 15 | 17 | 18 | OLD | OLD |
| 21800925 | 2019 | 2019-02-28 | Clint Capela | 1610612745 | 48.17 | 36.17 | 36.17 | 12.00 | -7 | 5 | 8 | OLD | OLD |
| 21900734 | 2020 | 2020-02-01 | Lonnie Walker IV | 1610612759 | 31.27 | 19.27 | 19.27 | 12.00 | 20 | 18 | 18 | OLD | OLD |
| 20600100 | 2007 | 2006-11-14 | Lorenzen Wright | 1610612737 | 33.02 | 21.02 | 21.02 | 12.00 | 27 | 11 | 11 | OLD | OLD |
| 21900673 | 2020 | 2020-01-24 | Kelly Olynyk | 1610612748 | 34.27 | 22.27 | 22.27 | 12.00 | 14 | 4 | 6 | OLD | OLD |
| 21901237 | 2020 | 2020-07-31 | Buddy Hield | 1610612758 | 31.52 | 19.52 | 19.52 | 12.00 | -18 | -15 | -13 | OLD | OLD |
| 21900019 | 2020 | 2019-10-25 | Rodions Kurucs | 1610612751 | 24.47 | 12.47 | 12.47 | 12.00 | 14 | 6 | 5 | OLD | OLD |
| 21901292 | 2020 | 2020-08-10 | Dario Šarić | 1610612756 | 34.92 | 22.92 | 22.92 | 12.00 | 25 | 9 | 9 | OLD | OLD |
| 21900797 | 2020 | 2020-02-10 | Buddy Hield | 1610612758 | 44.62 | 32.62 | 32.62 | 12.00 | -7 | -9 | -8 | OLD | TIE |
| 20200585 | 2003 | 2003-01-20 | Brian Skinner | 1610612755 | 30.12 | 18.13 | 18.13 | 11.98 | 12 | 6 | 5 | OLD | OLD |
| 21700598 | 2018 | 2018-01-08 | Mike Muscala | 1610612737 | 22.77 | 10.78 | 10.78 | 11.98 | 15 | 6 | 7 | OLD | OLD |
| 21900683 | 2020 | 2020-01-25 | Karl-Anthony Towns | 1610612750 | 47.42 | 35.43 | 35.43 | 11.98 | 3 | -8 | -7 | OLD | OLD |
| 21800988 | 2019 | 2019-03-09 | Taj Gibson | 1610612750 | 35.83 | 23.85 | 23.85 | 11.98 | -6 | 5 | 5 | OLD | OLD |
| 21900156 | 2020 | 2019-11-13 | Davis Bertans | 1610612764 | 39.20 | 27.22 | 27.22 | 11.98 | -3 | 0 | -3 | OLD | NEW |
| 20500038 | 2006 | 2005-11-05 | Jon Barry | 1610612745 | 37.35 | 25.37 | 25.37 | 11.98 | -3 | -9 | -9 | OLD | OLD |
| 40300303 | 2004 | 2004-05-26 | Metta World Peace | 1610612754 | 50.30 | 38.32 | 38.32 | 11.98 | -9 | -11 | -13 | OLD | OLD |
| 20800214 | 2009 | 2008-11-26 | Marreese Speights | 1610612755 | 25.50 | 13.53 | 13.53 | 11.97 | 14 | 4 | 3 | OLD | OLD |
| 21900821 | 2020 | 2020-02-20 | Kelly Olynyk | 1610612748 | 23.63 | 11.70 | 11.70 | 11.93 | -13 | -18 | -17 | OLD | OLD |
| 21900970 | 2020 | 2020-03-11 | Cody Zeller | 1610612766 | 34.40 | 22.47 | 22.47 | 11.93 | 34 | 7 | 8 | OLD | OLD |
| 20800508 | 2009 | 2009-01-06 | Luis Scola | 1610612745 | 48.30 | 36.38 | 36.38 | 11.92 | 4 | 5 | 5 | OLD | OLD |
| 21900415 | 2020 | 2019-12-18 | Hassan Whiteside | 1610612757 | 44.75 | 32.85 | 32.85 | 11.90 | -8 | 8 | 2 | OLD | OLD |
| 29700438 | 1998 | 1998-01-02 | Jim McIlvaine | 1610612760 | 29.80 | 17.92 | 17.92 | 11.88 | 21 | 10 | 9 | OLD | OLD |
| 29700438 | 1998 | 1998-01-02 | Derrick Coleman | 1610612755 | 34.07 | 22.23 | 22.23 | 11.83 | -33 | -19 | -21 | OLD | OLD |
| 21900123 | 2020 | 2019-11-08 | Mason Plumlee | 1610612743 | 22.05 | 10.23 | 10.23 | 11.82 | -19 | -13 | -15 | OLD | OLD |
| 21900278 | 2020 | 2019-11-29 | Skal Labissiere | 1610612757 | 29.67 | 17.85 | 17.85 | 11.82 | -2 | 6 | 3 | OLD | OLD |
| 29701075 | 1998 | 1998-04-05 | Tyus Edney | 1610612738 | 21.98 | 10.17 | 10.17 | 11.82 | 6 | 10 | 9 | OLD | OLD |
| 41900166 | 2020 | 2020-08-30 | Michael Porter Jr. | 1610612743 | 39.90 | 28.12 | 28.12 | 11.78 | 15 | 21 | 21 | OLD | OLD |
| 29700329 | 1998 | 1997-12-17 | Stanley Roberts | 1610612750 | 34.05 | 22.32 | 22.32 | 11.73 | 17 | 10 | 9 | OLD | OLD |
| 21900906 | 2020 | 2020-03-02 | Bam Adebayo | 1610612748 | 48.70 | 37.03 | 37.03 | 11.67 | 18 | 12 | 13 | OLD | OLD |
| 21900064 | 2020 | 2019-10-30 | Miles Bridges | 1610612766 | 45.60 | 33.97 | 33.97 | 11.63 | -1 | 0 | -1 | OLD | NEW |
| 21900427 | 2020 | 2019-12-20 | Robert Covington | 1610612750 | 34.73 | 23.15 | 23.15 | 11.58 | -5 | -1 | -1 | OLD | OLD |
| 41900315 | 2020 | 2020-09-26 | Dwight Howard | 1610612747 | 46.35 | 35.13 | 35.13 | 11.22 | 19 | 12 | 12 | OLD | OLD |
| 21900122 | 2020 | 2019-11-08 | Luka Dončić | 1610612742 | 46.42 | 35.20 | 35.20 | 11.22 | 1 | -4 | -5 | OLD | OLD |
| 29700452 | 1998 | 1998-01-04 | David Wingate | 1610612760 | 24.18 | 13.05 | 13.05 | 11.13 | 11 | -12 | -12 | OLD | OLD |
| 21900019 | 2020 | 2019-10-25 | Bobby Portis | 1610612752 | 21.02 | 9.93 | 9.93 | 11.08 | -4 | -1 | 0 | OLD | OLD |
| 29701075 | 1998 | 1998-04-05 | Chris Childs | 1610612752 | 26.73 | 15.68 | 15.68 | 11.05 | -7 | -10 | -10 | OLD | OLD |
| 29701075 | 1998 | 1998-04-05 | Terry Cummings | 1610612752 | 29.37 | 18.40 | 18.40 | 10.97 | 0 | -4 | -3 | OLD | OLD |
| 29700438 | 1998 | 1998-01-02 | Detlef Schrempf | 1610612760 | 31.75 | 20.83 | 20.83 | 10.92 | 27 | 14 | 15 | OLD | OLD |
| 21900249 | 2020 | 2019-11-25 | DeMar DeRozan | 1610612759 | 46.05 | 35.22 | 35.22 | 10.83 | -10 | -2 | -2 | OLD | OLD |
| 20400335 | 2005 | 2004-12-17 | Junior Harrington | 1610612740 | 36.95 | 26.17 | 26.17 | 10.78 | -22 | -11 | -11 | OLD | OLD |
| 29700161 | 1998 | 1997-11-22 | Bo Outlaw | 1610612753 | 45.80 | 35.22 | 35.22 | 10.58 | 4 | 15 | 14 | OLD | OLD |
| 41700222 | 2018 | 2018-05-02 | Clint Capela | 1610612745 | 41.30 | 30.98 | 30.98 | 10.32 | -3 | 2 | 4 | OLD | OLD |
| 21900583 | 2020 | 2020-01-11 | Nassir Little | 1610612757 | 23.72 | 13.52 | 13.52 | 10.20 | -25 | -12 | -13 | OLD | OLD |
| 41900114 | 2020 | 2020-08-23 | OG Anunoby | 1610612761 | 35.88 | 25.78 | 25.78 | 10.10 | 16 | 13 | 14 | OLD | OLD |
| 29700071 | 1998 | 1997-11-08 | Stacey Augmon | 1610612757 | 31.92 | 21.88 | 21.88 | 10.03 | 16 | 7 | 7 | OLD | OLD |
| 21900640 | 2020 | 2020-01-20 | Vince Carter | 1610612737 | 18.60 | 8.62 | 8.62 | 9.98 | -17 | -20 | -16 | OLD | NEW |
| 20900125 | 2010 | 2009-11-13 | Andre Iguodala | 1610612755 | 39.58 | 29.62 | 29.62 | 9.97 | -14 | -15 | -15 | OLD | OLD |
| 29800598 | 1999 | 1999-05-02 | Carl Herrera | 1610612743 | 9.93 | 0.00 | 9.93 | 9.93 | 0 | 0 | 0 | NEW | TIE |
| 29700697 | 1998 | 1998-02-11 | Rick Mahorn | 1610612765 | 28.12 | 18.28 | 18.28 | 9.83 | 26 | 19 | 20 | OLD | OLD |
| 21900201 | 2020 | 2019-11-19 | Nerlens Noel | 1610612760 | 33.43 | 23.85 | 23.85 | 9.58 | 0 | 0 | -1 | OLD | TIE |
| 21900535 | 2020 | 2020-01-05 | Anthony Tolliver | 1610612757 | 26.15 | 16.73 | 16.73 | 9.42 | -11 | -10 | -7 | OLD | OLD |
| 21900605 | 2020 | 2020-01-15 | Norvel Pelle | 1610612755 | 21.97 | 12.65 | 12.65 | 9.32 | -5 | -3 | -4 | OLD | TIE |
| 21900896 | 2020 | 2020-03-01 | Juancho Hernangomez | 1610612750 | 36.93 | 27.62 | 27.62 | 9.32 | -27 | -24 | -24 | OLD | OLD |
| 21900413 | 2020 | 2019-12-18 | Kristaps Porziņģis | 1610612742 | 46.77 | 37.58 | 37.58 | 9.18 | 16 | 5 | 6 | OLD | OLD |
| 21900833 | 2020 | 2020-02-21 | Kyle Anderson | 1610612763 | 31.32 | 22.28 | 22.28 | 9.03 | -25 | -20 | -17 | OLD | OLD |
| 21900384 | 2020 | 2019-12-14 | Dorian Finney-Smith | 1610612742 | 45.87 | 36.95 | 36.95 | 8.92 | -1 | -5 | -6 | OLD | OLD |
| 21900744 | 2020 | 2020-02-03 | Moritz Wagner | 1610612764 | 21.18 | 12.28 | 12.28 | 8.90 | 9 | 6 | 5 | OLD | OLD |
| 21901300 | 2020 | 2020-08-11 | Jusuf Nurkić | 1610612757 | 37.82 | 29.00 | 29.00 | 8.82 | -18 | -11 | -12 | OLD | OLD |
| 20401002 | 2005 | 2005-03-23 | Andre Iguodala | 1610612755 | 50.53 | 41.82 | 41.82 | 8.72 | 19 | 21 | 24 | OLD | OLD |
| 21900282 | 2020 | 2019-11-30 | Isaiah Hartenstein | 1610612745 | 29.42 | 20.78 | 20.78 | 8.63 | 36 | 15 | 15 | OLD | OLD |
| 21900814 | 2020 | 2020-02-12 | Maxi Kleber | 1610612742 | 26.80 | 18.48 | 18.48 | 8.32 | 5 | 5 | 4 | OLD | TIE |
| 29700103 | 1998 | 1997-11-14 | Andrew DeClercq | 1610612738 | 21.85 | 13.57 | 13.57 | 8.28 | 9 | 5 | 5 | OLD | OLD |
| 29700292 | 1998 | 1997-12-12 | Scott Williams | 1610612755 | 35.82 | 27.78 | 27.78 | 8.03 | 21 | 20 | 19 | OLD | OLD |
| 21700653 | 2018 | 2018-01-17 | Dwight Howard | 1610612766 | 36.92 | 28.90 | 28.90 | 8.02 | 18 | 22 | 17 | OLD | NEW |
| 21900894 | 2020 | 2020-02-29 | Marquese Chriss | 1610612744 | 33.67 | 25.68 | 25.68 | 7.98 | 22 | 18 | 15 | OLD | OLD |
| 20600603 | 2007 | 2007-01-21 | Francisco Elson | 1610612759 | 18.15 | 10.22 | 10.22 | 7.93 | 2 | 0 | 0 | OLD | OLD |
| 21900654 | 2020 | 2020-01-22 | Thon Maker | 1610612765 | 30.40 | 22.53 | 22.53 | 7.87 | 13 | 6 | 7 | OLD | OLD |
| 20800761 | 2009 | 2009-02-09 | Samuel Dalembert | 1610612755 | 31.12 | 23.40 | 23.40 | 7.72 | 28 | 20 | 21 | OLD | OLD |
| 21900762 | 2020 | 2020-02-05 | Royce O'Neale | 1610612762 | 27.88 | 20.27 | 20.27 | 7.62 | 1 | 1 | 1 | OLD | TIE |
| 21900783 | 2020 | 2020-02-08 | Zach Norvell Jr. | 1610612744 | 24.88 | 17.35 | 17.35 | 7.53 | 5 | 8 | 10 | OLD | OLD |
| 20501110 | 2006 | 2006-04-05 | Kevin Garnett | 1610612750 | 39.03 | 31.55 | 31.55 | 7.48 | 0 | 14 | 14 | OLD | OLD |
| 21900028 | 2020 | 2019-10-26 | Alex Len | 1610612737 | 26.97 | 19.65 | 19.65 | 7.32 | 9 | 1 | 1 | OLD | OLD |
| 29800063 | 1999 | 1999-02-12 | Hot Rod Williams | 1610612742 | 7.30 | 0.00 | 7.30 | 7.30 | -9 | -9 | -9 | NEW | TIE |
| 41900211 | 2020 | 2020-08-30 | Jayson Tatum | 1610612738 | 44.23 | 37.08 | 37.08 | 7.15 | 20 | 12 | 11 | OLD | OLD |
| 29800063 | 1999 | 1999-02-12 | Keon Clark | 1610612743 | 7.03 | 0.00 | 7.03 | 7.03 | -2 | -1 | -2 | NEW | NEW |
| 21300817 | 2014 | 2014-02-21 | Brandan Wright | 1610612742 | 21.37 | 14.58 | 14.58 | 6.78 | -2 | 4 | 4 | OLD | OLD |
| 29700233 | 1998 | 1997-12-03 | Nick Van Exel | 1610612747 | 40.60 | 33.95 | 33.95 | 6.65 | 8 | 1 | 2 | OLD | OLD |
| 41900103 | 2020 | 2020-08-22 | James Ennis III | 1610612753 | 15.42 | 9.07 | 9.07 | 6.35 | -25 | -14 | -14 | OLD | OLD |
| 21900375 | 2020 | 2019-12-13 | Daniel Gafford | 1610612741 | 26.53 | 20.30 | 20.30 | 6.23 | 16 | 9 | 10 | OLD | OLD |
| 21901300 | 2020 | 2020-08-11 | Zach Collins | 1610612757 | 26.08 | 19.88 | 19.88 | 6.20 | -20 | -17 | -14 | OLD | OLD |
| 21900487 | 2020 | 2019-12-29 | Jaren Jackson Jr. | 1610612763 | 31.63 | 25.47 | 25.47 | 6.17 | 22 | 20 | 18 | OLD | OLD |
| 21900439 | 2020 | 2019-12-22 | Dwight Powell | 1610612742 | 36.35 | 30.20 | 30.20 | 6.15 | 28 | 16 | 16 | OLD | OLD |
| 21800927 | 2019 | 2019-02-28 | Jonah Bolden | 1610612755 | 25.75 | 19.72 | 19.72 | 6.03 | 5 | 10 | 9 | OLD | OLD |
| 21900439 | 2020 | 2019-12-22 | Boban Marjanovic | 1610612742 | 7.97 | 2.12 | 2.12 | 5.85 | 2 | 2 | 2 | OLD | TIE |
| 21901300 | 2020 | 2020-08-11 | Carmelo Anthony | 1610612757 | 40.55 | 34.75 | 34.75 | 5.80 | 18 | 13 | 12 | OLD | OLD |
| 21900375 | 2020 | 2019-12-13 | Wendell Carter Jr. | 1610612741 | 29.35 | 23.58 | 23.58 | 5.77 | -18 | -16 | -17 | OLD | TIE |
| 41900103 | 2020 | 2020-08-22 | Gary Clark | 1610612753 | 40.40 | 34.75 | 34.75 | 5.65 | -23 | -15 | -15 | OLD | OLD |
| 29700233 | 1998 | 1997-12-03 | Eddie Jones | 1610612747 | 38.77 | 33.42 | 33.42 | 5.35 | 14 | 9 | 9 | OLD | OLD |
| 21800927 | 2019 | 2019-02-28 | Amir Johnson | 1610612755 | 19.10 | 13.85 | 13.85 | 5.25 | -5 | -7 | -7 | OLD | OLD |
| 21300817 | 2014 | 2014-02-21 | Samuel Dalembert | 1610612742 | 21.30 | 16.08 | 16.08 | 5.22 | -3 | 2 | 2 | OLD | OLD |
| 21200234 | 2013 | 2012-12-01 | Daniel Gibson | 1610612739 | 24.43 | 29.47 | 29.47 | -5.03 | 7 | 5 | 6 | OLD | TIE |
| 21901281 | 2020 | 2020-08-08 | Monté Morris | 1610612743 | 23.20 | 28.22 | 28.22 | -5.02 | -8 | -9 | -6 | OLD | NEW |
| 20400006 | 2005 | 2004-11-03 | Stephen Jackson | 1610612754 | 38.50 | 43.52 | 43.52 | -5.02 | 0 | 0 | 0 | OLD | TIE |
| 20300227 | 2004 | 2003-11-29 | J.R. Bremer | 1610612739 | 30.33 | 35.35 | 35.35 | -5.02 | -3 | -3 | -3 | OLD | TIE |
| 20900692 | 2010 | 2010-01-30 | Shawn Marion | 1610612742 | 22.43 | 27.45 | 27.45 | -5.02 | -7 | -7 | -9 | OLD | TIE |
| 21900120 | 2020 | 2019-11-08 | Treveon Graham | 1610612750 | 27.77 | 32.78 | 32.78 | -5.02 | -5 | 2 | 1 | OLD | OLD |
| 21000394 | 2011 | 2010-12-18 | Jamario Moon | 1610612739 | 15.82 | 20.83 | 20.83 | -5.02 | -4 | 5 | 3 | OLD | OLD |
| 20301041 | 2004 | 2004-03-26 | Josh Howard | 1610612742 | 16.42 | 21.43 | 21.43 | -5.02 | -4 | -4 | -5 | OLD | TIE |
| 20800666 | 2009 | 2009-01-27 | Lamar Odom | 1610612747 | 28.17 | 33.18 | 33.18 | -5.02 | -11 | -10 | -11 | OLD | NEW |
| 21400762 | 2015 | 2015-02-07 | Nicolas Batum | 1610612757 | 37.47 | 42.48 | 42.48 | -5.02 | -4 | -13 | -14 | OLD | OLD |
| 21600270 | 2017 | 2016-11-30 | Andre Roberson | 1610612760 | 30.02 | 35.02 | 35.02 | -5.00 | 1 | 16 | 12 | OLD | OLD |
| 20600887 | 2007 | 2007-03-04 | Rashad McCants | 1610612750 | 26.97 | 31.97 | 31.97 | -5.00 | -8 | -7 | -8 | OLD | NEW |
| 20201160 | 2003 | 2003-04-13 | Richard Hamilton | 1610612765 | 34.02 | 39.02 | 39.02 | -5.00 | -3 | -1 | -3 | OLD | NEW |
| 21401022 | 2015 | 2015-03-20 | Bojan Bogdanovic | 1610612751 | 43.52 | 48.52 | 48.52 | -5.00 | 6 | 5 | 6 | OLD | NEW |
| 21300240 | 2014 | 2013-11-29 | Darren Collison | 1610612746 | 35.12 | 40.12 | 40.12 | -5.00 | -5 | 2 | 1 | OLD | OLD |
| 20601061 | 2007 | 2007-03-28 | Delonte West | 1610612738 | 49.27 | 54.27 | 54.27 | -5.00 | 11 | 8 | 11 | OLD | NEW |
| 29701114 | 1998 | 1998-04-10 | Kevin Ollie | 1610612753 | 23.77 | 28.77 | 28.77 | -5.00 | 10 | 8 | 8 | OLD | OLD |
| 20100351 | 2002 | 2001-12-19 | Kerry Kittles | 1610612751 | 37.02 | 42.02 | 42.02 | -5.00 | -13 | -8 | -8 | OLD | OLD |
| 21100894 | 2012 | 2012-04-15 | Jason Kidd | 1610612742 | 33.67 | 38.67 | 38.67 | -5.00 | 2 | -2 | -2 | OLD | OLD |
| 20000923 | 2001 | 2001-03-13 | Moochie Norris | 1610612745 | 32.52 | 37.52 | 37.52 | -5.00 | 18 | 18 | 18 | OLD | TIE |
| 20900912 | 2010 | 2010-03-04 | Metta World Peace | 1610612747 | 39.87 | 44.87 | 44.87 | -5.00 | -2 | -5 | -5 | OLD | OLD |
| 20000494 | 2001 | 2001-01-09 | Baron Davis | 1610612766 | 47.87 | 52.87 | 52.87 | -5.00 | -1 | 5 | 6 | OLD | OLD |
| 20800373 | 2009 | 2008-12-17 | Al Thornton | 1610612746 | 43.37 | 48.37 | 48.37 | -5.00 | 1 | -5 | -5 | OLD | OLD |
| 21600281 | 2017 | 2016-12-01 | Draymond Green | 1610612744 | 41.92 | 46.92 | 46.92 | -5.00 | 1 | 4 | 1 | OLD | NEW |
| 21001007 | 2011 | 2011-03-16 | Carlos Delfino | 1610612749 | 15.02 | 20.02 | 20.02 | -5.00 | -5 | -8 | -9 | OLD | OLD |
| 21800216 | 2019 | 2018-11-16 | Marcus Morris Sr. | 1610612738 | 28.07 | 33.07 | 33.07 | -5.00 | 4 | 12 | 11 | OLD | OLD |
| 21800143 | 2019 | 2018-11-05 | Noah Vonleh | 1610612752 | 29.57 | 34.57 | 34.57 | -5.00 | -3 | 0 | -4 | OLD | NEW |
| 21800371 | 2019 | 2018-12-07 | Pascal Siakam | 1610612761 | 31.67 | 36.67 | 36.67 | -5.00 | 3 | 1 | 2 | OLD | TIE |
| 20700876 | 2008 | 2008-03-02 | Jerry Stackhouse | 1610612742 | 26.42 | 31.42 | 31.42 | -5.00 | 10 | 8 | 6 | OLD | OLD |
| 20200619 | 2003 | 2003-01-25 | Corey Benjamin | 1610612737 | 26.32 | 31.32 | 31.32 | -5.00 | -1 | -6 | 0 | OLD | NEW |
| 21400004 | 2015 | 2014-10-29 | Jared Dudley | 1610612749 | 21.17 | 26.17 | 26.17 | -5.00 | 0 | -1 | -2 | OLD | OLD |
| 29800065 | 1999 | 1999-02-13 | Clifford Robinson | 1610612756 | 27.97 | 32.97 | 32.97 | -5.00 | -1 | 2 | 2 | OLD | OLD |
| 20400932 | 2005 | 2005-03-14 | Tom Gugliotta | 1610612737 | 28.57 | 33.57 | 33.57 | -5.00 | -4 | -17 | -10 | OLD | NEW |
| 21900892 | 2020 | 2020-02-29 | Eric Gordon | 1610612745 | 16.42 | 21.42 | 21.42 | -5.00 | -2 | -4 | -1 | OLD | NEW |
| 20900912 | 2010 | 2010-03-04 | Quentin Richardson | 1610612748 | 31.97 | 36.97 | 36.97 | -5.00 | -1 | 0 | 2 | OLD | OLD |
| 20600271 | 2007 | 2006-12-07 | Eddie House | 1610612751 | 26.62 | 31.62 | 31.62 | -5.00 | 1 | 1 | 1 | OLD | TIE |
| 20701224 | 2008 | 2008-04-16 | Charlie Villanueva | 1610612749 | 30.07 | 35.07 | 35.07 | -5.00 | -11 | -18 | -20 | OLD | OLD |
| 20700449 | 2008 | 2007-12-31 | Matt Carroll | 1610612766 | 29.43 | 24.43 | 24.43 | 5.00 | 15 | 11 | 11 | OLD | OLD |
| 21600559 | 2017 | 2017-01-08 | Allen Crabbe | 1610612757 | 37.57 | 42.57 | 42.57 | -5.00 | -6 | -7 | -7 | OLD | OLD |
| 20200839 | 2003 | 2003-02-28 | Shane Battier | 1610612763 | 34.97 | 39.97 | 39.97 | -5.00 | 0 | 8 | 10 | OLD | OLD |
| 20100916 | 2002 | 2002-03-13 | Darrell Armstrong | 1610612753 | 35.32 | 40.32 | 40.32 | -5.00 | -2 | 3 | 3 | OLD | OLD |
| 20100517 | 2002 | 2002-01-12 | Jason Williams | 1610612763 | 34.97 | 39.97 | 39.97 | -5.00 | -4 | 0 | -2 | OLD | TIE |
| 20000931 | 2001 | 2001-03-14 | Danny Ferry | 1610612759 | 34.57 | 39.57 | 39.57 | -5.00 | 4 | 8 | 10 | OLD | OLD |
| 40200206 | 2003 | 2003-05-16 | Derrick Coleman | 1610612755 | 42.47 | 47.47 | 47.47 | -5.00 | 2 | -2 | -2 | OLD | OLD |
| 21600281 | 2017 | 2016-12-01 | Andre Iguodala | 1610612744 | 36.72 | 41.72 | 41.72 | -5.00 | 14 | 14 | 14 | OLD | TIE |
| 20800142 | 2009 | 2008-11-16 | Zach Randolph | 1610612752 | 42.07 | 47.07 | 47.07 | -5.00 | 9 | -2 | -1 | OLD | OLD |
| 40200233 | 2003 | 2003-05-10 | Michael Finley | 1610612742 | 50.82 | 55.82 | 55.82 | -5.00 | -3 | 1 | 1 | OLD | OLD |
| 21000615 | 2011 | 2011-01-19 | Jrue Holiday | 1610612755 | 34.57 | 39.57 | 39.57 | -5.00 | -5 | -2 | -6 | OLD | NEW |
| 21700607 | 2018 | 2018-01-10 | Michael Beasley | 1610612752 | 33.97 | 38.97 | 38.97 | -5.00 | 0 | 0 | 0 | OLD | TIE |
| 29800100 | 1999 | 1999-02-17 | Sam Mack | 1610612763 | 44.22 | 49.22 | 49.22 | -5.00 | -2 | -3 | -2 | OLD | NEW |
| 21500515 | 2016 | 2016-01-04 | Goran Dragic | 1610612748 | 33.57 | 38.57 | 38.57 | -5.00 | -8 | -5 | -5 | OLD | OLD |
| 20900363 | 2010 | 2009-12-16 | Derek Fisher | 1610612747 | 30.70 | 35.70 | 35.70 | -5.00 | 9 | 10 | 10 | OLD | OLD |
| 40200141 | 2003 | 2003-04-19 | Joe Johnson | 1610612756 | 31.95 | 36.95 | 36.95 | -5.00 | 4 | 3 | 5 | OLD | NEW |
| 20801120 | 2009 | 2009-04-01 | C.J. Watson | 1610612744 | 29.45 | 34.45 | 34.45 | -5.00 | 8 | 10 | 10 | OLD | OLD |
| 21800569 | 2019 | 2019-01-04 | Wendell Carter Jr. | 1610612741 | 37.50 | 42.50 | 42.50 | -5.00 | 10 | 7 | 7 | OLD | OLD |
| 21900892 | 2020 | 2020-02-29 | Jeff Green | 1610612745 | 18.75 | 13.75 | 13.75 | 5.00 | -10 | -13 | -11 | OLD | NEW |
| 21800881 | 2019 | 2019-02-22 | Joe Ingles | 1610612762 | 41.30 | 46.30 | 46.30 | -5.00 | 9 | 9 | 9 | OLD | TIE |
| 21401076 | 2015 | 2015-03-27 | Marcin Gortat | 1610612764 | 39.10 | 44.10 | 44.10 | -5.00 | -6 | -4 | -6 | OLD | NEW |
| 21000086 | 2011 | 2010-11-06 | Andrei Kirilenko | 1610612762 | 42.90 | 47.90 | 47.90 | -5.00 | -3 | -3 | -3 | OLD | TIE |
| 20700599 | 2008 | 2008-01-21 | James Jones | 1610612757 | 24.00 | 29.00 | 29.00 | -5.00 | 12 | 14 | 14 | OLD | OLD |
| 41200313 | 2013 | 2013-05-25 | Manu Ginobili | 1610612759 | 24.70 | 29.70 | 29.70 | -5.00 | 2 | 13 | 13 | OLD | OLD |
| 41200134 | 2013 | 2013-04-27 | Kirk Hinrich | 1610612741 | 54.60 | 59.60 | 59.60 | -5.00 | 12 | 12 | 12 | OLD | TIE |
| 20000341 | 2001 | 2000-12-16 | Wally Szczerbiak | 1610612750 | 43.10 | 48.10 | 48.10 | -5.00 | 12 | 12 | 15 | OLD | TIE |
| 21100661 | 2012 | 2012-03-17 | David Lee | 1610612744 | 34.35 | 39.35 | 39.35 | -5.00 | -1 | -7 | -8 | OLD | OLD |
| 20000494 | 2001 | 2001-01-09 | Bryce Drew | 1610612741 | 41.15 | 46.15 | 46.15 | -5.00 | 10 | 13 | 10 | OLD | NEW |
| 20100505 | 2002 | 2002-01-11 | Quentin Richardson | 1610612746 | 23.60 | 28.60 | 28.60 | -5.00 | -10 | -12 | -11 | OLD | TIE |
| 20100517 | 2002 | 2002-01-12 | Grant Long | 1610612763 | 42.75 | 47.75 | 47.75 | -5.00 | -2 | 1 | 0 | OLD | OLD |
| 20801223 | 2009 | 2009-04-15 | Joel Anthony | 1610612748 | 32.20 | 37.20 | 37.20 | -5.00 | 6 | 6 | 12 | OLD | TIE |
| 20201104 | 2003 | 2003-04-06 | Tony Battie | 1610612738 | 21.15 | 26.15 | 26.15 | -5.00 | -3 | -1 | -4 | OLD | NEW |
| 20700541 | 2008 | 2008-01-13 | Carlos Delfino | 1610612761 | 39.95 | 44.95 | 44.95 | -5.00 | 5 | 12 | 12 | OLD | OLD |
| 21500523 | 2016 | 2016-01-05 | Wesley Matthews | 1610612742 | 37.60 | 42.60 | 42.60 | -5.00 | -1 | -1 | 0 | OLD | TIE |
| 20700449 | 2008 | 2007-12-31 | Jeff McInnis | 1610612766 | 32.40 | 37.40 | 37.40 | -5.00 | -10 | -7 | -6 | OLD | OLD |
| 20200769 | 2003 | 2003-02-18 | Robert Horry | 1610612747 | 48.75 | 53.75 | 53.75 | -5.00 | 11 | 11 | 11 | OLD | TIE |
| 20700107 | 2008 | 2007-11-14 | Daniel Gibson | 1610612739 | 36.45 | 41.45 | 41.45 | -5.00 | 5 | 4 | 4 | OLD | OLD |
| 20701051 | 2008 | 2008-03-24 | Stephen Jackson | 1610612744 | 48.00 | 53.00 | 53.00 | -5.00 | 0 | -4 | -4 | OLD | OLD |
| 20600628 | 2007 | 2007-01-24 | Mike Miller | 1610612763 | 40.75 | 45.75 | 45.75 | -5.00 | -5 | -2 | -3 | OLD | OLD |
| 20401139 | 2005 | 2005-04-09 | Brent Barry | 1610612759 | 25.00 | 20.00 | 20.00 | 5.00 | -2 | -12 | -2 | OLD | NEW |
| 20401139 | 2005 | 2005-04-09 | Bruce Bowen | 1610612759 | 35.85 | 40.85 | 40.85 | -5.00 | -2 | 2 | -2 | OLD | NEW |
| 20500696 | 2006 | 2006-02-05 | Cuttino Mobley | 1610612746 | 43.10 | 48.10 | 48.10 | -5.00 | 3 | 4 | 5 | OLD | OLD |
| 20500350 | 2006 | 2005-12-19 | Shane Battier | 1610612763 | 44.25 | 49.25 | 49.25 | -5.00 | 5 | 3 | 3 | OLD | OLD |
| 21400424 | 2015 | 2014-12-23 | Steve Blake | 1610612757 | 29.90 | 34.90 | 34.90 | -5.00 | 1 | 5 | 5 | OLD | OLD |
| 20500182 | 2006 | 2005-11-26 | John Salmons | 1610612755 | 20.00 | 25.00 | 25.00 | -5.00 | 11 | 8 | 8 | OLD | OLD |
| 21600976 | 2017 | 2017-03-11 | Solomon Hill | 1610612740 | 38.35 | 43.35 | 43.35 | -5.00 | 5 | 6 | 8 | OLD | OLD |
| 29800065 | 1999 | 1999-02-13 | Jason Kidd | 1610612756 | 45.20 | 50.20 | 50.20 | -5.00 | 5 | 8 | 8 | OLD | OLD |
| 21800143 | 2019 | 2018-11-05 | Justin Holiday | 1610612741 | 41.85 | 46.85 | 46.85 | -5.00 | 2 | 1 | 3 | OLD | NEW |
| 21500624 | 2016 | 2016-01-18 | Paul Pierce | 1610612746 | 20.95 | 25.95 | 25.95 | -5.00 | 7 | 13 | 15 | OLD | OLD |
| 21700482 | 2018 | 2017-12-23 | Tyler Zeller | 1610612751 | 26.25 | 21.25 | 21.25 | 5.00 | -2 | 3 | 2 | OLD | OLD |
| 21801070 | 2019 | 2019-03-20 | Justin Holiday | 1610612763 | 43.85 | 38.85 | 38.85 | 5.00 | -13 | -12 | -14 | OLD | NEW |
| 21800371 | 2019 | 2018-12-07 | Joe Harris | 1610612751 | 30.55 | 35.55 | 35.55 | -5.00 | -2 | -1 | -1 | OLD | OLD |
| 20700535 | 2008 | 2008-01-12 | Nazr Mohammed | 1610612766 | 36.55 | 31.55 | 31.55 | 5.00 | -6 | -5 | -3 | OLD | OLD |
| 40800116 | 2009 | 2009-04-30 | John Salmons | 1610612741 | 54.93 | 59.93 | 59.93 | -5.00 | 3 | 5 | 4 | OLD | TIE |
| 20401119 | 2005 | 2005-04-08 | Josh Childress | 1610612737 | 42.28 | 47.28 | 47.28 | -5.00 | -4 | -13 | -12 | OLD | OLD |
| 41500134 | 2016 | 2016-04-24 | Jae Crowder | 1610612738 | 35.78 | 40.78 | 40.78 | -5.00 | -4 | 8 | 5 | OLD | OLD |
| 29700157 | 1998 | 1997-11-21 | Brent Barry | 1610612746 | 38.43 | 43.43 | 43.43 | -5.00 | 9 | 0 | 0 | OLD | OLD |
| 20601081 | 2007 | 2007-03-30 | Shane Battier | 1610612745 | 40.53 | 45.53 | 45.53 | -5.00 | -1 | 3 | 2 | OLD | OLD |
| 20000590 | 2001 | 2001-01-23 | Mike Miller | 1610612753 | 41.43 | 46.43 | 46.43 | -5.00 | -10 | -2 | -5 | OLD | OLD |
| 20800440 | 2009 | 2008-12-27 | Shane Battier | 1610612745 | 35.28 | 40.28 | 40.28 | -5.00 | 3 | 6 | 8 | OLD | OLD |
| 20100840 | 2002 | 2002-03-03 | Mike Miller | 1610612753 | 40.03 | 45.03 | 45.03 | -5.00 | -6 | -13 | -11 | OLD | OLD |
| 20801227 | 2009 | 2009-04-15 | Roger Mason Jr. | 1610612759 | 32.43 | 37.43 | 37.43 | -5.00 | 4 | 11 | 11 | OLD | OLD |
| 20500295 | 2006 | 2005-12-12 | Trenton Hassell | 1610612750 | 35.18 | 40.18 | 40.18 | -5.00 | 7 | 6 | 6 | OLD | OLD |
| 20501088 | 2006 | 2006-04-02 | Rashad McCants | 1610612750 | 39.53 | 44.53 | 44.53 | -5.00 | -2 | -1 | 0 | OLD | OLD |
| 49800015 | 1999 | 1999-05-11 | Jalen Rose | 1610612754 | 29.93 | 34.93 | 34.93 | -5.00 | 16 | 16 | 17 | OLD | TIE |
| 29901035 | 2000 | 2000-03-31 | Chucky Brown | 1610612766 | 30.18 | 35.18 | 35.18 | -5.00 | 7 | 16 | 16 | OLD | OLD |
| 20800666 | 2009 | 2009-01-27 | Sasha Vujacic | 1610612747 | 24.68 | 29.68 | 29.68 | -5.00 | -1 | -1 | -1 | OLD | TIE |
| 20100162 | 2002 | 2001-11-21 | Richard Jefferson | 1610612751 | 22.63 | 27.63 | 27.63 | -5.00 | 7 | 8 | 8 | OLD | OLD |
| 21700482 | 2018 | 2017-12-23 | Allen Crabbe | 1610612751 | 29.78 | 34.78 | 34.78 | -5.00 | 9 | 8 | 5 | OLD | OLD |
| 29700157 | 1998 | 1997-11-21 | Rusty LaRue | 1610612741 | 26.13 | 31.13 | 31.13 | -5.00 | 23 | 22 | 23 | OLD | NEW |
| 20001024 | 2001 | 2001-03-27 | Mark Jackson | 1610612752 | 28.78 | 33.78 | 33.78 | -5.00 | -4 | -14 | -11 | OLD | OLD |
| 20200769 | 2003 | 2003-02-18 | Rick Fox | 1610612747 | 38.73 | 43.73 | 43.73 | -5.00 | 12 | 12 | 12 | OLD | TIE |
| 29700621 | 1998 | 1998-01-29 | Charles Oakley | 1610612752 | 39.88 | 44.88 | 44.88 | -5.00 | 5 | 2 | 2 | OLD | OLD |
| 20700527 | 2008 | 2008-01-11 | Daniel Gibson | 1610612739 | 35.38 | 40.38 | 40.38 | -5.00 | 17 | 17 | 17 | OLD | TIE |
| 21900696 | 2020 | 2020-01-27 | Harrison Barnes | 1610612758 | 33.98 | 38.98 | 38.98 | -5.00 | 0 | 3 | 4 | OLD | OLD |
| 20200992 | 2003 | 2003-03-21 | Rashard Lewis | 1610612760 | 45.63 | 50.63 | 50.63 | -5.00 | -2 | -1 | 0 | OLD | OLD |
| 21801070 | 2019 | 2019-03-20 | Bruno Caboclo | 1610612763 | 35.88 | 40.88 | 40.88 | -5.00 | -1 | 3 | 0 | OLD | NEW |
| 20400516 | 2005 | 2005-01-13 | Nenad Krstic | 1610612751 | 39.98 | 44.98 | 44.98 | -5.00 | 3 | -6 | -6 | OLD | OLD |
| 20500717 | 2006 | 2006-02-08 | Steve Francis | 1610612753 | 40.88 | 45.88 | 45.88 | -5.00 | 7 | 0 | 2 | OLD | OLD |
| 21801132 | 2019 | 2019-03-29 | Andre Iguodala | 1610612744 | 24.98 | 29.98 | 29.98 | -5.00 | 1 | 3 | 0 | OLD | NEW |
| 20500498 | 2006 | 2006-01-10 | Earl Watson | 1610612743 | 41.98 | 46.98 | 46.98 | -5.00 | 8 | 10 | 8 | OLD | NEW |
| 29800026 | 1999 | 1999-02-07 | Michael Finley | 1610612742 | 44.23 | 49.23 | 49.23 | -5.00 | 4 | 5 | 7 | OLD | OLD |
| 21700692 | 2018 | 2018-01-22 | Darius Miller | 1610612740 | 35.58 | 40.58 | 40.58 | -5.00 | 10 | 13 | 14 | OLD | OLD |
| 20400528 | 2005 | 2005-01-14 | Bobby Simmons | 1610612746 | 41.98 | 46.98 | 46.98 | -5.00 | 5 | 3 | 5 | OLD | NEW |
| 41900161 | 2020 | 2020-08-17 | Royce O'Neale | 1610612762 | 26.78 | 31.78 | 31.78 | -5.00 | -11 | -16 | -21 | OLD | OLD |
| 21400436 | 2015 | 2014-12-26 | Courtney Lee | 1610612763 | 34.63 | 39.63 | 39.63 | -5.00 | 3 | -3 | -3 | OLD | OLD |
| 21900272 | 2020 | 2019-11-29 | De'Andre Hunter | 1610612737 | 35.73 | 40.73 | 40.73 | -5.00 | 11 | 9 | 10 | OLD | TIE |
| 21001007 | 2011 | 2011-03-16 | Keyon Dooling | 1610612749 | 37.12 | 32.12 | 32.12 | 5.00 | 5 | 9 | 9 | OLD | OLD |
| 21100094 | 2012 | 2012-01-05 | Udonis Haslem | 1610612748 | 41.73 | 46.73 | 46.73 | -5.00 | -10 | -10 | -10 | OLD | TIE |
| 20000460 | 2001 | 2001-01-04 | Chris Childs | 1610612752 | 32.13 | 37.13 | 37.13 | -5.00 | -6 | -2 | -3 | OLD | OLD |
| 20801227 | 2009 | 2009-04-15 | Rasual Butler | 1610612740 | 36.38 | 41.38 | 41.38 | -5.00 | -5 | -13 | -12 | OLD | OLD |
| 20000494 | 2001 | 2001-01-09 | P.J. Brown | 1610612766 | 40.33 | 45.33 | 45.33 | -5.00 | -13 | -5 | -6 | OLD | OLD |
| 21000048 | 2011 | 2010-11-02 | Kirk Hinrich | 1610612764 | 37.23 | 42.23 | 42.23 | -5.00 | -15 | -14 | -14 | OLD | OLD |
| 20100162 | 2002 | 2001-11-21 | Kerry Kittles | 1610612751 | 28.38 | 33.38 | 33.38 | -5.00 | -10 | -7 | -9 | OLD | NEW |
| 20800065 | 2009 | 2008-11-06 | Metta World Peace | 1610612745 | 40.48 | 45.48 | 45.48 | -5.00 | 1 | 0 | -1 | OLD | OLD |
| 21300257 | 2014 | 2013-12-02 | Kirk Hinrich | 1610612741 | 48.13 | 53.13 | 53.13 | -5.00 | -4 | -9 | -7 | OLD | OLD |
| 21200754 | 2013 | 2013-02-10 | Jeff Green | 1610612738 | 36.63 | 41.63 | 41.63 | -5.00 | -9 | -7 | -5 | OLD | OLD |
| 20700204 | 2008 | 2007-11-27 | Daniel Gibson | 1610612739 | 41.53 | 46.52 | 46.52 | -4.98 | 0 | 6 | 5 | OLD | OLD |
| 20201104 | 2003 | 2003-04-06 | Eric Williams | 1610612738 | 34.72 | 29.73 | 29.73 | 4.98 | 3 | 4 | 4 | OLD | OLD |
| 21200652 | 2013 | 2013-01-27 | Jason Terry | 1610612738 | 27.13 | 32.12 | 32.12 | -4.98 | 1 | 2 | 1 | OLD | NEW |
| 21100641 | 2012 | 2012-03-15 | Jamaal Tinsley | 1610612762 | 25.92 | 20.93 | 20.93 | 4.98 | 17 | 11 | 11 | OLD | OLD |
| 21100641 | 2012 | 2012-03-15 | Alec Burks | 1610612762 | 26.18 | 31.17 | 31.17 | -4.98 | 2 | 8 | 8 | OLD | OLD |
| 21100094 | 2012 | 2012-01-05 | James Jones | 1610612748 | 32.52 | 27.53 | 27.53 | 4.98 | 6 | 7 | 6 | OLD | NEW |
| 41200114 | 2013 | 2013-04-28 | Avery Bradley | 1610612738 | 34.92 | 39.90 | 39.90 | -4.98 | 1 | 7 | 8 | OLD | OLD |
| 21500721 | 2016 | 2016-02-01 | JR Smith | 1610612739 | 37.12 | 42.10 | 42.10 | -4.98 | 2 | 7 | 7 | OLD | OLD |
| 21900409 | 2020 | 2019-12-18 | Ish Smith | 1610612764 | 28.37 | 33.35 | 33.35 | -4.98 | 14 | 11 | 13 | OLD | NEW |
| 29901035 | 2000 | 2000-03-31 | Todd Fuller | 1610612766 | 15.23 | 10.25 | 10.25 | 4.98 | -3 | -10 | -12 | OLD | OLD |
| 41200134 | 2013 | 2013-04-27 | C.J. Watson | 1610612751 | 35.93 | 30.95 | 30.95 | 4.98 | -10 | -10 | -10 | OLD | TIE |
| 29900693 | 2000 | 2000-02-09 | Doug Christie | 1610612761 | 34.07 | 39.05 | 39.05 | -4.98 | 5 | 0 | -2 | OLD | OLD |
| 20700535 | 2008 | 2008-01-12 | Matt Carroll | 1610612766 | 38.32 | 43.30 | 43.30 | -4.98 | -9 | -8 | -12 | OLD | NEW |
| 40700161 | 2008 | 2008-04-19 | Leandro Barbosa | 1610612756 | 35.57 | 40.55 | 40.55 | -4.98 | 2 | 1 | 0 | OLD | OLD |
| 21500587 | 2016 | 2016-01-14 | Hollis Thompson | 1610612755 | 28.55 | 33.53 | 33.53 | -4.98 | 11 | 5 | 7 | OLD | OLD |
| 29700899 | 1998 | 1998-03-12 | Hubert Davis | 1610612742 | 33.50 | 38.48 | 38.48 | -4.98 | 5 | 11 | 12 | OLD | OLD |
| 21900563 | 2020 | 2020-01-09 | Tony Snell | 1610612765 | 30.60 | 35.58 | 35.58 | -4.98 | 5 | 2 | 2 | OLD | OLD |
| 21400302 | 2015 | 2014-12-08 | Bradley Beal | 1610612764 | 40.75 | 45.73 | 45.73 | -4.98 | 0 | 3 | 0 | OLD | NEW |
| 41200134 | 2013 | 2013-04-27 | Reggie Evans | 1610612751 | 45.00 | 49.98 | 49.98 | -4.98 | 5 | 4 | 5 | OLD | NEW |
| 21701085 | 2018 | 2018-03-23 | Pau Gasol | 1610612759 | 16.90 | 11.93 | 11.93 | 4.97 | -3 | -5 | -5 | OLD | OLD |
| 21800143 | 2019 | 2018-11-05 | Mario Hezonja | 1610612752 | 40.97 | 36.00 | 36.00 | 4.97 | 2 | 4 | 2 | OLD | NEW |
| 40700161 | 2008 | 2008-04-19 | Grant Hill | 1610612756 | 33.17 | 28.20 | 28.20 | 4.97 | -8 | -6 | -6 | OLD | OLD |
| 41900211 | 2020 | 2020-08-30 | Brad Wanamaker | 1610612738 | 33.23 | 28.38 | 28.38 | 4.85 | -1 | 3 | 5 | OLD | OLD |
| 20600887 | 2007 | 2007-03-04 | Marko Jaric | 1610612750 | 23.60 | 18.83 | 18.83 | 4.77 | -7 | -8 | -4 | OLD | NEW |
| 20600887 | 2007 | 2007-03-04 | Craig Smith | 1610612750 | 24.32 | 29.08 | 29.08 | -4.77 | -8 | -17 | -18 | OLD | OLD |
| 29800606 | 1999 | 1999-04-21 | Vinny Del Negro | 1610612749 | 25.67 | 20.92 | 20.92 | 4.75 | -14 | -6 | -8 | OLD | OLD |
| 21900028 | 2020 | 2019-10-26 | John Collins | 1610612737 | 37.75 | 33.07 | 33.07 | 4.68 | 7 | 9 | 10 | OLD | OLD |
| 29700876 | 1998 | 1998-03-08 | Scot Pollard | 1610612765 | 4.67 | 0.00 | 4.67 | 4.67 | -5 | -6 | -5 | NEW | NEW |
| 21900696 | 2020 | 2020-01-27 | Dewayne Dedmon | 1610612758 | 20.37 | 15.83 | 15.83 | 4.53 | -15 | -20 | -17 | OLD | NEW |
| 21700007 | 2018 | 2017-10-18 | Daniel Theis | 1610612738 | 9.28 | 4.75 | 4.75 | 4.53 | 4 | 2 | 2 | OLD | OLD |
| 20501110 | 2006 | 2006-04-05 | Justin Reed | 1610612750 | 23.32 | 18.80 | 18.80 | 4.52 | -14 | -8 | -8 | OLD | OLD |
| 21900783 | 2020 | 2020-02-08 | Damion Lee | 1610612744 | 26.87 | 22.38 | 22.38 | 4.48 | 1 | -5 | -5 | OLD | OLD |
| 21900487 | 2020 | 2019-12-29 | Solomon Hill | 1610612763 | 25.53 | 21.13 | 21.13 | 4.40 | -4 | -5 | -2 | OLD | NEW |
| 21900762 | 2020 | 2020-02-05 | Joe Ingles | 1610612762 | 34.77 | 30.38 | 30.38 | 4.38 | -4 | 2 | 2 | OLD | OLD |
| 20400516 | 2005 | 2005-01-13 | Jabari Smith | 1610612751 | 22.98 | 18.67 | 18.67 | 4.32 | -19 | -18 | -14 | OLD | OLD |
| 20800761 | 2009 | 2009-02-09 | Theo Ratliff | 1610612755 | 8.40 | 4.18 | 4.18 | 4.22 | -2 | -1 | -1 | OLD | OLD |
| 21900654 | 2020 | 2020-01-22 | Derrick Rose | 1610612765 | 34.25 | 30.12 | 30.12 | 4.13 | 2 | -3 | 0 | OLD | NEW |
| 20600603 | 2007 | 2007-01-21 | Robert Horry | 1610612759 | 26.28 | 22.22 | 22.22 | 4.07 | 13 | 10 | 10 | OLD | OLD |
| 20800142 | 2009 | 2008-11-16 | David Lee | 1610612752 | 40.20 | 36.17 | 36.17 | 4.03 | -26 | -16 | -16 | OLD | OLD |
| 21700653 | 2018 | 2018-01-17 | Malik Monk | 1610612766 | 7.97 | 3.98 | 3.98 | 3.98 | 0 | 0 | 2 | OLD | TIE |
| 29700292 | 1998 | 1997-12-12 | Terry Cummings | 1610612755 | 18.30 | 14.33 | 14.33 | 3.97 | 7 | 2 | 2 | OLD | OLD |
| 21700007 | 2018 | 2017-10-18 | Marcus Smart | 1610612738 | 36.23 | 32.33 | 32.33 | 3.90 | -8 | -2 | -4 | OLD | OLD |
| 29700103 | 1998 | 1997-11-14 | Travis Knight | 1610612738 | 36.63 | 32.92 | 32.92 | 3.72 | -2 | -3 | -5 | OLD | OLD |
| 21900814 | 2020 | 2020-02-12 | Willie Cauley-Stein | 1610612742 | 7.40 | 3.70 | 3.70 | 3.70 | -2 | -2 | -1 | OLD | TIE |
| 21700007 | 2018 | 2017-10-18 | Aron Baynes | 1610612738 | 19.53 | 15.97 | 15.97 | 3.57 | -12 | -5 | -7 | OLD | OLD |
| 20401002 | 2005 | 2005-03-23 | Willie Green | 1610612755 | 6.57 | 3.28 | 3.28 | 3.28 | 8 | 5 | 4 | OLD | OLD |
| 21901300 | 2020 | 2020-08-11 | Hassan Whiteside | 1610612757 | 22.08 | 18.93 | 18.93 | 3.15 | 21 | 12 | 15 | OLD | OLD |
| 21900384 | 2020 | 2019-12-14 | Justin Jackson | 1610612742 | 15.58 | 12.48 | 12.48 | 3.10 | 10 | 6 | 5 | OLD | OLD |
| 21900744 | 2020 | 2020-02-03 | Gary Payton II | 1610612764 | 14.57 | 11.48 | 11.48 | 3.08 | -25 | -17 | -16 | OLD | OLD |
| 21900833 | 2020 | 2020-02-21 | Brandon Clarke | 1610612763 | 35.47 | 32.50 | 32.50 | 2.97 | 3 | 9 | 8 | OLD | OLD |
| 21900413 | 2020 | 2019-12-18 | Maxi Kleber | 1610612742 | 24.12 | 21.30 | 21.30 | 2.82 | -21 | -16 | -16 | OLD | OLD |
| 21900605 | 2020 | 2020-01-15 | Mike Scott | 1610612755 | 14.07 | 11.38 | 11.38 | 2.68 | 1 | 1 | 1 | OLD | TIE |
| 21900896 | 2020 | 2020-03-01 | Kelan Martin | 1610612750 | 14.75 | 12.07 | 12.07 | 2.68 | 0 | 1 | 1 | OLD | OLD |
| 21900535 | 2020 | 2020-01-05 | Carmelo Anthony | 1610612757 | 39.93 | 37.33 | 37.33 | 2.60 | -12 | -5 | -9 | OLD | NEW |
| 21900201 | 2020 | 2019-11-19 | Steven Adams | 1610612760 | 26.57 | 24.15 | 24.15 | 2.42 | -1 | -5 | -4 | OLD | OLD |
| 21900894 | 2020 | 2020-02-29 | Eric Paschall | 1610612744 | 34.83 | 32.53 | 32.53 | 2.30 | 18 | 18 | 18 | OLD | TIE |
| 29700697 | 1998 | 1998-02-11 | Eric Montross | 1610612765 | 6.30 | 4.13 | 4.13 | 2.17 | -5 | -4 | -2 | OLD | OLD |
| 20900125 | 2010 | 2009-11-13 | Rodney Carney | 1610612755 | 13.27 | 11.23 | 11.23 | 2.03 | -12 | -12 | -12 | OLD | TIE |
| 21900640 | 2020 | 2020-01-20 | John Collins | 1610612737 | 39.05 | 37.03 | 37.03 | 2.02 | -5 | 2 | -2 | OLD | NEW |
| 29700071 | 1998 | 1997-11-08 | Brian Grant | 1610612757 | 35.32 | 33.35 | 33.35 | 1.97 | -1 | -2 | -3 | OLD | OLD |
| 41900114 | 2020 | 2020-08-23 | Matt Thomas | 1610612761 | 26.47 | 24.57 | 24.57 | 1.90 | 6 | 5 | 6 | OLD | NEW |
| 21900583 | 2020 | 2020-01-11 | Gary Trent Jr. | 1610612757 | 19.25 | 17.45 | 17.45 | 1.80 | 3 | -2 | -1 | OLD | OLD |
| 21900894 | 2020 | 2020-02-29 | Juan Toscano-Anderson | 1610612744 | 32.58 | 30.87 | 30.87 | 1.72 | -5 | -4 | -7 | OLD | NEW |
| 21900282 | 2020 | 2019-11-30 | P.J. Tucker | 1610612745 | 31.42 | 29.73 | 29.73 | 1.68 | 40 | 38 | 39 | OLD | TIE |
| 41700222 | 2018 | 2018-05-02 | Ryan Anderson | 1610612745 | 8.57 | 6.88 | 6.88 | 1.68 | 7 | 2 | 1 | OLD | OLD |
| 21900282 | 2020 | 2019-11-30 | Gary Clark | 1610612745 | 26.10 | 24.42 | 24.42 | 1.68 | 21 | 16 | 18 | OLD | OLD |
| 21900487 | 2020 | 2019-12-29 | Jonas Valančiūnas | 1610612763 | 24.28 | 22.85 | 22.85 | 1.43 | 13 | 15 | 16 | OLD | OLD |
| 29700161 | 1998 | 1997-11-22 | Brian Evans | 1610612753 | 8.20 | 6.78 | 6.78 | 1.42 | -11 | -10 | -9 | OLD | OLD |
| 29800075 | 1999 | 1999-02-15 | Kendall Gill | 1610612751 | 31.37 | 32.72 | 32.72 | -1.35 | -5 | -8 | -5 | OLD | NEW |
| 21900249 | 2020 | 2019-11-25 | Lonnie Walker IV | 1610612759 | 2.33 | 1.17 | 1.17 | 1.17 | 4 | 2 | 2 | OLD | OLD |
| 20800142 | 2009 | 2008-11-16 | Nate Robinson | 1610612752 | 29.40 | 28.43 | 28.43 | 0.97 | -8 | -6 | -8 | OLD | NEW |
| 21900019 | 2020 | 2019-10-25 | Marcus Morris Sr. | 1610612752 | 34.23 | 33.32 | 33.32 | 0.92 | -6 | 1 | -1 | OLD | OLD |
| 21900122 | 2020 | 2019-11-08 | Kristaps Porziņģis | 1610612742 | 36.38 | 35.60 | 35.60 | 0.78 | -6 | -2 | -4 | OLD | TIE |
| 41900315 | 2020 | 2020-09-26 | Kentavious Caldwell-Pope | 1610612747 | 27.15 | 26.37 | 26.37 | 0.78 | 5 | 6 | 5 | OLD | NEW |
| 21800927 | 2019 | 2019-02-28 | T.J. McConnell | 1610612755 | 17.77 | 17.05 | 17.05 | 0.72 | 8 | 5 | 6 | OLD | OLD |
| 29700159 | 1998 | 1997-11-21 | Priest Lauderdale | 1610612743 | 5.70 | 6.37 | 6.37 | -0.67 | 4 | 6 | 4 | OLD | NEW |
| 20400516 | 2005 | 2005-01-13 | Travis Best | 1610612751 | 21.82 | 21.15 | 21.15 | 0.67 | -22 | -15 | -19 | OLD | NEW |
| 29700003 | 1998 | 1997-10-31 | Jeff Nordgaard | 1610612749 | 0.55 | 0.00 | 0.57 | 0.55 | 0 | -1 | 0 | NEW | NEW |
| 29700003 | 1998 | 1997-10-31 | Tim Breaux | 1610612749 | 0.55 | 0.00 | 0.57 | 0.55 | 0 | -1 | 0 | NEW | NEW |
| 20000383 | 2001 | 2000-12-22 | Vlade Divac | 1610612758 | 33.57 | 33.03 | 33.03 | 0.53 | 0 | -1 | 0 | OLD | NEW |
| 20000383 | 2001 | 2000-12-22 | Michael Dickerson | 1610612763 | 40.58 | 40.05 | 40.05 | 0.53 | 7 | 4 | 7 | OLD | NEW |
| 20000383 | 2001 | 2000-12-22 | Ike Austin | 1610612763 | 33.48 | 32.95 | 32.95 | 0.53 | 8 | 5 | 8 | OLD | NEW |
| 20000383 | 2001 | 2000-12-22 | Othella Harrington | 1610612763 | 40.30 | 39.77 | 39.77 | 0.53 | 5 | 6 | 5 | OLD | NEW |
| 20000383 | 2001 | 2000-12-22 | Shareef Abdur-Rahim | 1610612763 | 41.90 | 41.38 | 41.38 | 0.52 | -14 | -15 | -14 | OLD | NEW |
| 20000383 | 2001 | 2000-12-22 | Mike Bibby | 1610612763 | 42.15 | 41.63 | 41.63 | 0.52 | 5 | 1 | 5 | OLD | NEW |
| 20000383 | 2001 | 2000-12-22 | Doug Christie | 1610612758 | 35.30 | 34.78 | 34.78 | 0.52 | -6 | -6 | -6 | OLD | TIE |
| 20000383 | 2001 | 2000-12-22 | Chris Webber | 1610612758 | 43.97 | 43.45 | 43.45 | 0.52 | 5 | 3 | 5 | OLD | NEW |
| 20000383 | 2001 | 2000-12-22 | Peja Stojakovic | 1610612758 | 40.47 | 39.95 | 39.95 | 0.52 | 0 | 0 | 0 | OLD | TIE |
| 20000383 | 2001 | 2000-12-22 | Jason Williams | 1610612758 | 23.58 | 23.07 | 23.07 | 0.52 | -11 | -11 | -11 | OLD | TIE |

---

## Appendix C: Expanded column audit (2026-03-16)

Systematic comparison of all 152 shared numeric columns beyond the 14 basic counting stats. Joined on `Game_SingleGame` + `NbaDotComID` (596,351 matched player-game rows).

### Finding 1: Shooting zone classification mismatch (CRITICAL)

**NEW file FAILS the zone FGM sanity check.** `0_3ft_FGM + 4_9ft_FGM + 10_17ft_FGM + 18_23ft_FGM + 3PM ≠ FGM` for 323,496 rows (54.2%). The OLD file passes perfectly (0 mismatches).

The gap is typically 0-3 FGM (mean +0.22), meaning the NEW pipeline loses ~0.2 made field goals per player-game from zone assignment.

**Root cause**: The NEW pipeline assigns zone buckets using `field_goal.darko_distance_bucket`, which uses numeric `self.distance`. When `distance` is `None` (no coordinate data), the shot gets no zone assignment. The OLD pipeline likely uses pbpstats `shot_type` (AtRim, ShortMidRange, LongMidRange) which is always populated.

Zone-level differences:

| Zone | Rows Different | % | Direction in NEW |
|------|---------------|---|-----------------|
| 0_3ft_FGA | 267,402 | 44.8% | Systematically LOWER (mean -1.27) |
| 0_3ft_FGM | 222,464 | 37.3% | Systematically LOWER (mean -0.77) |
| 18_23ft_FGA | 320,530 | 53.7% | Systematically HIGHER (mean +0.92) |
| 18_23ft_FGM | 191,791 | 32.2% | Systematically HIGHER (mean +0.56) |
| 4_9ft_FGA | 3,121 | 0.5% | Small noise |
| 4_9ft_FGM | 1,708 | 0.3% | Small noise |
| 10_17ft_FGA | 2,269 | 0.4% | Small noise |
| 10_17ft_FGM | 1,251 | 0.2% | Small noise |

The 0_3ft and 18_23ft zones bear nearly all the difference. The 4_9ft and 10_17ft zones are clean (<0.5%). This suggests the NEW pipeline reclassifies some shots between the 0-3ft and 18-23ft buckets (possibly due to different distance cutoff thresholds vs shot_type labels), AND drops some shots entirely when distance data is missing.

**Season pattern**:
- 1998-2010: 52-57% of rows affected (worst period)
- 2011-2014: drops to 13-16%
- 2015-2017: rises to ~29%
- 2018-2020: drops to 8-9%

This tracks NBA data quality improvements — earlier seasons had more missing shot coordinates.

### Finding 2: TSAttempts uses different formula (INTENTIONAL?)

**59.2% of rows differ.** The NEW pipeline computes TSAttempts via `_compute_ts_attempts_exact()` (line 502 of v9b.py), which counts individual FGA events and qualifying FT trips from PBP events. The OLD pipeline uses the standard formula `FGA + 0.44 * FTA`.

Evidence: the OLD file satisfies `TSAttempts = FGA + 0.44*FTA` with zero deviation. The NEW file deviates by up to 10.56. Even on rows where FGA and FTA match between files, 353,096 rows (59.2%) have different TSAttempts.

The event-based approach counts each FT trip as 1.0 (filtering by `is_first_ft` and excluding and-1s, shooting fouls, flagrants, technicals — line 512-516). This differs from the `0.44 * FTA` approximation. The typical diff is a multiple of 0.44 (0.88, 1.32, 1.76), confirming the FTA-weighting difference.

**This cascades into TSpct** (58.9% of rows differ), since `TSpct = PTS / (2 * TSAttempts)`.

### Finding 3: PF_DRAWN — era-dependent (pre-2006 vs post-2006)

**17.4% of rows differ overall**, but the pattern is sharply era-dependent:

| Era | Diff Rate | Explanation |
|-----|-----------|-------------|
| 1998-2005 | 50-53% | PBP data doesn't reliably tag foul-drawn player |
| 2006-2020 | 0.6-0.9% | Negligible — both pipelines agree |

NEW is systematically LOWER (mean diff -0.32), suggesting the NEW pipeline doesn't count foul-drawn events that the OLD pipeline inferred from context. This is only a problem for pre-2006 data.

### Finding 4: On-court FT stats diverge much more than FGA stats

On-court opponent/team FGA and 3PA differ on <0.2% of rows, but FT stats differ on 40-56%:

| Column | % Different |
|--------|------------|
| OnCourt_Team_FGA | Not tested separately (but low) |
| OnCourt_Opp_FGA | 0.16% |
| OnCourt_Team_FT_Att | 45.8% |
| OnCourt_Team_FT_Made | 39.9% |
| OnCourt_Opp_FT_Att | 55.9% |
| OnCourt_Opp_FT_Made | 49.6% |
| OnCourt_Team_3p_Att | 0.10% |
| OnCourt_Opp_3p_Att | 0.11% |

FTs happen during dead balls when substitutions frequently occur, so lineup tracking differences (the same root cause as Plus_Minus) have a disproportionate effect on FT attribution.

The rebound opportunity bases (`OnCourt_For_OREB_FGA` 98.0%, `OnCourt_For_DREB_FGA` 90.0%) differ on nearly every row. These are on-court context stats that depend heavily on lineup tracking quality.

### Finding 5: POSS_OFF and POSS_DEF are individually ~1:1

Contrary to what the POSS (2x factor) might suggest, the individual components POSS_OFF and POSS_DEF have a mean OLD/NEW ratio of ~0.998. The 2x factor in POSS is purely from the `/2` on line 812 of v9b.py. The 50-56% diff rate on POSS_OFF/POSS_DEF reflects small lineup-tracking noise, not a systematic multiplier.

### Finding 6: Other PBP counting stats — mostly clean

| Stat | Rows Diff | % | Note |
|------|----------|---|------|
| TOV_Live | 113 | 0.02% | Clean |
| TOV_Dead | 73 | 0.01% | Clean |
| PF_Loose | 3 | 0.001% | Clean |
| CHRG | 107 | 0.02% | Clean |
| TECH | 2,487 | 0.42% | Small diffs |
| BLK_Opp | 30,165 | 5.1% | Moderate |
| BLK_Team | 38,861 | 6.5% | Moderate |
| AndOnes | 5,168 | 0.87% | Small diffs |
| Starts | 40 | 0.007% | Clean |
| G / DNP | 238/104 | 0.04% | Clean |

`BLK_Opp` (5.1%) and `BLK_Team` (6.5%) are moderately different. `BLK_Opp` is computed as `total_blk_pbp - BLK_Team` (line 821), so both stem from how blocks are attributed to teams in PBP parsing.

### Finding 7: Assisted/unassisted splits — zone-correlated

The assisted/unassisted splits follow the zone pattern: 0_3ft and 18_23ft zones show 23-49% diff rates, while 4_9ft and 10_17ft show <0.5%. 3-point assisted/unassisted is nearly perfect (<0.01%). The overall `FGM_AST` and `FGM_UNAST` differ 23% due to the zone reclassification propagating into the assist breakdowns.

### Finding 8: Derived percentages — mostly follow from inputs

Simple shooting percentages (`FGPct` 0.03%, `3PPct` 0.007%, `FT%` 0.004%) are near-perfect — they follow from the clean counting stats.

Rates that use possession denominators diverge as expected:
- `DRBPct` 70.8%, `USG` 51.1%, `ORBpct` 45.7% — driven by POSS_OFF/POSS_DEF noise + lineup tracking
- `BLKPct` 30.2%, `STLpct` 7.7% — moderate, driven by POSS_DEF denominator + BLK differences

### Sanity check results

| Check | NEW | OLD |
|-------|-----|-----|
| FGM = sum(zone FGMs) + 3PM | **FAIL** (323,496 mismatches) | PASS |
| FGM_AST + FGM_UNAST = FGM | PASS | PASS |
| TOV_Live + TOV_Dead = TOV | FAIL (2 rows) | PASS |

### Finding 9: Zone sanity check failure is TWO bugs, not one

Deeper investigation reveals the zone sanity check failure (`FGM ≠ zones + 3PM`) is caused by two compounding issues:

**Bug A: 3-pointers are double-counted in zones.** `darko_distance_bucket` classifies ALL field goals by distance, including 3-pointers. A 3-pointer at 24ft gets `18to23Ft` via the catch-all at line 131 of `field_goal.py`. But `3PM` is separately sourced from boxscore `FG3M`. So in the sanity check `zones + 3PM`, made 3-pointers appear in BOTH `18_23ft_FGM` and `3PM`.

Confirmed by data: `(FGM - 3PM) - zone_sum` is negative for 146,219 rows — meaning zones contain more than just 2-point FGMs.

**Bug B: `distance=None` drops 2-point shots from zones.** When `self.distance` is `None` (no coordinate data, description unparseable), `darko_distance_bucket` returns `None` and the shot vanishes from all zone stats.

Confirmed by data: `(FGM - 3PM) - zone_sum` is positive for 193,847 rows — meaning zones are missing some 2-point FGMs.

**Net effect**: The 194K undercounted rows outweigh the 146K double-counted rows, producing the observed net undercount of ~0.26 FGM per player-game.

The OLD pipeline avoids both bugs because its zones are 2pt-only (mapped from `shot_type` labels like `AtRim`, `ShortMidRange`, `LongMidRange`) and `shot_type` always returns a value for 2-pointers (falling back to `UnknownDistance2pt` when distance is missing).

**Fix**: (a) Return `None` for 3-pointers in `darko_distance_bucket` (make zones 2pt-only), (b) Fall back to `shot_type` for 2-pointers when `distance` is `None`.

### Finding 10: TSAttempts filter logic is inverted (BUG)

The event-based `_compute_ts_attempts_exact()` (v9b.py line 502-519) has an inverted filter. It counts FT trips only when `free_throw_type` does NOT contain "shooting foul", "and 1", "flagrant", or "technical":

```python
if "and 1" in ft_type or "shooting foul" in ft_type or "flagrant" in ft_type or "technical" in ft_type:
    continue  # SKIP these
```

This is backwards:
- **Shooting fouls** are the primary FT-generating events — they SHOULD count toward TSA
- **And-1 FTs** correctly should NOT add to TSA (the FGA already counts)
- **Flagrant FTs** SHOULD count (they're shooting fouls with bonus FTs)
- **Technical FTs** arguably should not count (no FGA involved)

So the filter keeps the wrong types (penalty fouls, away-from-play fouls, etc.) and discards the right types. This produces TSAttempts values that differ from the standard formula on 59% of rows.

The standard formula `FGA + 0.44 * FTA` is universally used, matches the OLD pipeline perfectly, and should be restored.

### Summary of action items from expanded audit

1. **Zone classification — two bugs (CRITICAL)**: (a) `darko_distance_bucket` should return `None` for 3-pointers (make zones 2pt-only), (b) fall back to `shot_type` when `distance` is `None` for 2-pointers. This affects 54% of rows and cascades into all zone-level stats, assisted/unassisted splits, and zone shooting percentages. Fix location: `pbpstats/resources/enhanced_pbp/field_goal.py` lines 113-131.

2. **TSAttempts formula — inverted filter (BUG)**: Replace `_compute_ts_attempts_exact()` with standard `FGA + 0.44 * FTA`. The event-based function's filter logic is inverted, excluding the FT types it should include. Fix location: `0c2_build_tpdev_box_stats_version_v9b.py` lines 502-519, 713, 814 (and duplicate at 1103, 1202).

3. **PF_DRAWN pre-2006**: The 50% difference in 1998-2005 may be acceptable if the NEW pipeline's PBP parsing is more conservative about attributing drawn fouls. Needs review of whether the OLD pipeline was over-counting or the NEW pipeline is under-counting.

### Columns with zero differences

`FLAGRANT`, `Goaltends`, `h_tm_id`, `season`, `v_tm_id` — identical across both files.