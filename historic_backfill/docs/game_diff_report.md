# Game-Level Difference Report: NEW vs OLD Boxscore Data

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

