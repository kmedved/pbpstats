"""
Expanded column-by-column audit: darko_1997_2020.parquet vs tpdev_box_new.parq
Compares all shared numeric columns beyond the 14 basic counting stats already audited.
"""
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BUNDLE_ROOT = ROOT.parent
TPDEV_DIR = BUNDLE_ROOT / "fixed_data" / "raw_input_data" / "tpdev_data"

NEW_PATH = ROOT / "darko_1997_2020.parquet"
OLD_PATH = TPDEV_DIR / "tpdev_box_new.parq"

# Already audited columns (from existing audit)
ALREADY_AUDITED = {
    "PTS", "AST", "FGM", "FGA", "FTM", "FTA", "3PM", "3PA",
    "OREB", "DRB", "STL", "TOV", "BLK", "PF",
    "Minutes", "Plus_Minus", "POSS", "Pace",
    "FLAGRANT", "Goaltends", "h_tm_id", "season", "v_tm_id",
}

# Join keys and metadata (not stats)
SKIP_COLS = {
    "Game_SingleGame", "NbaDotComID", "Date", "Team_SingleGame",
    "FullName", "Player_Code", "Year", "Position", "Source",
    "home_fl",
}

# Group definitions for reporting
PRIORITY_GROUPS = {
    "P1: PBP counting stats": [
        "TOV_Live", "TOV_Dead", "PF_DRAWN", "PF_Loose",
        "CHRG", "TECH", "BLK_Opp", "AndOnes", "Starts", "G", "DNP",
        "BLK_Team", "PossessionsUsed", "TSAttempts",
    ],
    "P2: Shooting zone splits": [
        "0_3ft_FGM", "0_3ft_FGA",
        "4_9ft_FGM", "4_9ft_FGA",
        "10_17ft_FGM", "10_17ft_FGA",
        "18_23ft_FGM", "18_23ft_FGA",
    ],
    "P3: Assisted / unassisted": [
        "FGM_AST", "FGM_UNAST", "FGA_UNAST",
        "3PM_AST", "3PM_UNAST", "3PA_UNAST",
        "0_3ft_FGM_AST", "0_3ft_FGM_UNAST", "0_3ft_FGA_UNAST",
        "4_9ft_FGM_AST", "4_9ft_FGM_UNAST", "4_9ft_FGA_UNAST",
        "10_17ft_FGM_AST", "10_17ft_FGM_UNAST", "10_17ft_FGA_UNAST",
        "18_23ft_FGM_AST", "18_23ft_FGM_UNAST", "18_23ft_FGA_UNAST",
    ],
    "P4: On-court team/opponent": [
        "OnCourt_Team_FGM", "OnCourt_Team_FGA",
        "OnCourt_Team_3p_Made", "OnCourt_Team_3p_Att",
        "OnCourt_Team_FT_Made", "OnCourt_Team_FT_Att",
        "OnCourt_Team_Points",
        "OnCourt_Opp_FGA", "OnCourt_Opp_3p_Made", "OnCourt_Opp_3p_Att",
        "OnCourt_Opp_FT_Made", "OnCourt_Opp_FT_Att",
        "OnCourt_Opp_Points",
        "OnCourt_For_OREB_FGA", "OnCourt_For_DREB_FGA",
        "TM_BLK_OnCourt",
    ],
    "P5: Raw possession components": [
        "POSS_OFF", "POSS_DEF", "Seconds_Off", "Seconds_Def",
    ],
    "P6: Derived percentages/rates": [
        "FGPct", "3PPct", "FT%", "TSpct", "USG",
        "ASTpct", "BLKPct", "STLpct", "TOVpct",
        "ORBpct", "DRBPct",
        "FTR_Att", "FTR_Made",
    ],
    "P7: Rebound splits": [
        "OREB_FGA", "OREB_FT", "DRB_FGA", "DRB_FT",
        "OREBPct_FGA", "OREBPct_FT", "DRBPct_FGA", "DRBPct_FT",
        "DREB_FGA", "DREB_FT",
    ],
}

# Columns that use rates/percentages (need smaller tolerance)
RATE_COLS = {
    "FGPct", "3PPct", "FT%", "TSpct", "USG", "ASTpct", "BLKPct",
    "STLpct", "TOVpct", "ORBpct", "DRBPct", "FTR_Att", "FTR_Made",
    "OREBPct_FGA", "OREBPct_FT", "DRBPct_FGA", "DRBPct_FT",
}

def load_and_join():
    print("Loading NEW file...")
    new = pd.read_parquet(NEW_PATH)
    print(f"  {new.shape[0]:,} rows x {new.shape[1]} cols")

    print("Loading OLD file...")
    old = pd.read_parquet(OLD_PATH)
    old = old[old["season"] <= 2020]
    print(f"  {old.shape[0]:,} rows x {old.shape[1]} cols (filtered to <=2020)")

    # Filter to player rows only (exclude team aggregates)
    new_players = new[new["NbaDotComID"] != 0].copy()
    old_players = old[old["NbaDotComID"] != 0].copy()
    print(f"  NEW player rows: {new_players.shape[0]:,}")
    print(f"  OLD player rows: {old_players.shape[0]:,}")

    merged = new_players.merge(
        old_players,
        on=["Game_SingleGame", "NbaDotComID"],
        suffixes=("_new", "_old"),
        how="inner",
    )
    print(f"  Matched rows: {merged.shape[0]:,}")
    return new, old, merged

def compare_column(merged, col, tol=0.01):
    col_new = f"{col}_new"
    col_old = f"{col}_old"
    if col_new not in merged.columns or col_old not in merged.columns:
        return None

    v_new = pd.to_numeric(merged[col_new], errors="coerce").fillna(0)
    v_old = pd.to_numeric(merged[col_old], errors="coerce").fillna(0)
    diff = (v_new - v_old).abs()
    differs = diff > tol
    n_diff = differs.sum()
    pct = 100.0 * n_diff / len(merged)
    max_diff = diff.max()

    # Find example game with largest diff
    example = ""
    if n_diff > 0:
        idx = diff.idxmax()
        game = merged.loc[idx, "Game_SingleGame"]
        example = str(game)

    return {
        "column": col,
        "rows_diff": int(n_diff),
        "pct_diff": round(pct, 3),
        "max_abs_diff": round(float(max_diff), 4),
        "example_game": example,
    }

def run_sanity_checks(merged):
    """Run cross-column sanity checks on the NEW file."""
    checks = []

    # FGM = sum of zone FGMs + we need to check if 3PM is separate
    zone_fgm_cols = ["0_3ft_FGM_new", "4_9ft_FGM_new", "10_17ft_FGM_new", "18_23ft_FGM_new", "3PM_new"]
    if all(c in merged.columns for c in zone_fgm_cols):
        zone_sum = sum(merged[c].fillna(0) for c in zone_fgm_cols)
        fgm = merged["FGM_new"].fillna(0)
        mismatches = (zone_sum - fgm).abs() > 0.01
        checks.append({
            "check": "FGM = 0_3ft + 4_9ft + 10_17ft + 18_23ft + 3PM (NEW)",
            "mismatches": int(mismatches.sum()),
            "total": len(merged),
        })

    # Same check for OLD
    zone_fgm_old = ["0_3ft_FGM_old", "4_9ft_FGM_old", "10_17ft_FGM_old", "18_23ft_FGM_old", "3PM_old"]
    if all(c in merged.columns for c in zone_fgm_old):
        zone_sum = sum(merged[c].fillna(0) for c in zone_fgm_old)
        fgm = merged["FGM_old"].fillna(0)
        mismatches = (zone_sum - fgm).abs() > 0.01
        checks.append({
            "check": "FGM = 0_3ft + 4_9ft + 10_17ft + 18_23ft + 3PM (OLD)",
            "mismatches": int(mismatches.sum()),
            "total": len(merged),
        })

    # FGM_AST + FGM_UNAST = FGM
    for label, sfx in [("NEW", "_new"), ("OLD", "_old")]:
        ast_col = f"FGM_AST{sfx}"
        unast_col = f"FGM_UNAST{sfx}"
        fgm_col = f"FGM{sfx}"
        if all(c in merged.columns for c in [ast_col, unast_col, fgm_col]):
            total = merged[ast_col].fillna(0) + merged[unast_col].fillna(0)
            mismatches = (total - merged[fgm_col].fillna(0)).abs() > 0.01
            checks.append({
                "check": f"FGM_AST + FGM_UNAST = FGM ({label})",
                "mismatches": int(mismatches.sum()),
                "total": len(merged),
            })

    # TOV_Live + TOV_Dead = TOV
    for label, sfx in [("NEW", "_new"), ("OLD", "_old")]:
        live_col = f"TOV_Live{sfx}"
        dead_col = f"TOV_Dead{sfx}"
        tov_col = f"TOV{sfx}"
        if all(c in merged.columns for c in [live_col, dead_col, tov_col]):
            total = merged[live_col].fillna(0) + merged[dead_col].fillna(0)
            mismatches = (total - merged[tov_col].fillna(0)).abs() > 0.01
            checks.append({
                "check": f"TOV_Live + TOV_Dead = TOV ({label})",
                "mismatches": int(mismatches.sum()),
                "total": len(merged),
            })

    return checks

def find_shared_columns(new, old):
    """Find all shared columns."""
    return sorted(set(new.columns) & set(old.columns))

def main():
    new, old, merged = load_and_join()

    shared = find_shared_columns(new, old)
    print(f"\nShared columns: {len(shared)}")

    # Identify numeric columns
    numeric_shared = []
    for col in shared:
        if col in ALREADY_AUDITED or col in SKIP_COLS:
            continue
        col_new = f"{col}_new"
        if col_new in merged.columns:
            if pd.api.types.is_numeric_dtype(merged[col_new]):
                numeric_shared.append(col)

    print(f"Numeric columns to audit: {len(numeric_shared)}")

    # Build reverse lookup: col -> group
    col_to_group = {}
    for group, cols in PRIORITY_GROUPS.items():
        for c in cols:
            col_to_group[c] = group

    # Compare all columns
    results_by_group = {}
    ungrouped = []

    for col in numeric_shared:
        tol = 0.001 if col in RATE_COLS else 0.01
        result = compare_column(merged, col, tol=tol)
        if result is None:
            continue

        group = col_to_group.get(col, None)
        if group:
            results_by_group.setdefault(group, []).append(result)
        else:
            ungrouped.append(result)

    # Also check _100p columns
    for col in numeric_shared:
        if "_100p" in col and col not in col_to_group:
            # These are rate columns affected by POSS denominator - already documented
            pass

    # Print results
    print("\n" + "=" * 90)
    print("EXPANDED AUDIT RESULTS")
    print("=" * 90)

    for group_name in PRIORITY_GROUPS:
        results = results_by_group.get(group_name, [])
        if not results:
            print(f"\n### {group_name}: no shared columns found")
            continue

        print(f"\n### {group_name}")
        print(f"{'Column':<30} {'Rows Diff':>10} {'%':>8} {'Max Diff':>10} {'Example Game'}")
        print("-" * 85)
        for r in sorted(results, key=lambda x: -x["rows_diff"]):
            print(f"{r['column']:<30} {r['rows_diff']:>10,} {r['pct_diff']:>7.3f}% {r['max_abs_diff']:>10.4f} {r['example_game']}")

    if ungrouped:
        # Separate _100p columns from truly ungrouped
        hundredp = [r for r in ungrouped if "_100p" in r["column"]]
        truly_ungrouped = [r for r in ungrouped if "_100p" not in r["column"]]

        if truly_ungrouped:
            print(f"\n### Ungrouped columns")
            print(f"{'Column':<30} {'Rows Diff':>10} {'%':>8} {'Max Diff':>10} {'Example Game'}")
            print("-" * 85)
            for r in sorted(truly_ungrouped, key=lambda x: -x["rows_diff"]):
                print(f"{r['column']:<30} {r['rows_diff']:>10,} {r['pct_diff']:>7.3f}% {r['max_abs_diff']:>10.4f} {r['example_game']}")

        if hundredp:
            print(f"\n### Per-100-possession rate columns (_100p) — affected by POSS denominator")
            # Just show summary stats
            diffs = [r["pct_diff"] for r in hundredp]
            print(f"  {len(hundredp)} columns, diff rates: min={min(diffs):.3f}%, max={max(diffs):.3f}%, median={np.median(diffs):.3f}%")
            # Show top 5
            print(f"  Top 5 by diff rate:")
            for r in sorted(hundredp, key=lambda x: -x["rows_diff"])[:5]:
                print(f"    {r['column']:<35} {r['rows_diff']:>10,} {r['pct_diff']:>7.3f}%")

    # Sanity checks
    print("\n" + "=" * 90)
    print("SANITY CHECKS")
    print("=" * 90)
    checks = run_sanity_checks(merged)
    for c in checks:
        status = "PASS" if c["mismatches"] == 0 else f"FAIL ({c['mismatches']:,} mismatches)"
        print(f"  {c['check']}: {status}")

    # List columns only in one file
    new_only = sorted(set(new.columns) - set(old.columns))
    old_only = sorted(set(old.columns) - set(new.columns))
    print(f"\n### Columns only in NEW ({len(new_only)}): {new_only}")
    print(f"### Columns only in OLD ({len(old_only)}): {old_only}")

    # Summary of columns with zero differences
    all_results = []
    for results in results_by_group.values():
        all_results.extend(results)
    all_results.extend(ungrouped)

    zero_diff = [r["column"] for r in all_results if r["rows_diff"] == 0]
    if zero_diff:
        print(f"\n### Columns with ZERO differences ({len(zero_diff)}): {zero_diff}")

if __name__ == "__main__":
    main()
