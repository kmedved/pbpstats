"""Deep dive into the biggest findings from the expanded audit."""
from pathlib import Path

import pandas as pd
import numpy as np

ROOT = Path(__file__).resolve().parent
TPDEV = ROOT.parent / "fixed_data" / "raw_input_data" / "tpdev_data"

print("Loading files...")
new = pd.read_parquet(ROOT / "darko_1997_2020.parquet")
old = pd.read_parquet(TPDEV / "tpdev_box_new.parq")
old = old[old['season'] <= 2020]

new_p = new[new['NbaDotComID'] != 0]
old_p = old[old['NbaDotComID'] != 0]
m = new_p.merge(old_p, on=['Game_SingleGame','NbaDotComID'], suffixes=('_new','_old'), how='inner')

# ============================================================
# 1) Zone FGM sanity check — what's the distribution?
# ============================================================
print("\n" + "=" * 70)
print("1) ZONE FGM SANITY CHECK")
print("=" * 70)

zone_sum_new = (m['0_3ft_FGM_new'].fillna(0) + m['4_9ft_FGM_new'].fillna(0) +
                m['10_17ft_FGM_new'].fillna(0) + m['18_23ft_FGM_new'].fillna(0) +
                m['3PM_new'].fillna(0))
gap_new = m['FGM_new'].fillna(0) - zone_sum_new
print("NEW: FGM - sum(zone_FGM + 3PM) distribution:")
print(gap_new.describe())
print("\nValue counts (top 10):")
print(gap_new.value_counts().head(10))

# Check if OLD passes
zone_sum_old = (m['0_3ft_FGM_old'].fillna(0) + m['4_9ft_FGM_old'].fillna(0) +
                m['10_17ft_FGM_old'].fillna(0) + m['18_23ft_FGM_old'].fillna(0) +
                m['3PM_old'].fillna(0))
gap_old = m['FGM_old'].fillna(0) - zone_sum_old
print("\nOLD: FGM - sum(zone_FGM + 3PM) distribution:")
print(gap_old.describe())
print("\nValue counts (top 5):")
print(gap_old.value_counts().head(5))

# ============================================================
# 2) 0_3ft and 18_23ft — are they offset from each other?
# ============================================================
print("\n" + "=" * 70)
print("2) 0_3ft vs 18_23ft ZONE RECLASSIFICATION?")
print("=" * 70)

d_03 = m['0_3ft_FGM_new'].fillna(0) - m['0_3ft_FGM_old'].fillna(0)
d_18 = m['18_23ft_FGM_new'].fillna(0) - m['18_23ft_FGM_old'].fillna(0)

print("0_3ft_FGM diff (NEW - OLD):")
print(d_03.describe())
print("\nValue counts:")
print(d_03.value_counts().head(8))

print("\n18_23ft_FGM diff (NEW - OLD):")
print(d_18.describe())
print("\nValue counts:")
print(d_18.value_counts().head(8))

# Do they cancel?
mask = (d_03.abs() > 0.01) | (d_18.abs() > 0.01)
print(f"\nRows where either differs: {mask.sum():,}")
net = d_03 + d_18
print(f"Correlation of d_03 and d_18 (where either differs): {d_03[mask].corr(d_18[mask]):.4f}")
print(f"Sum (d_03 + d_18) — if ~0 they cancel out:")
print(net[mask].describe())

# By season
print("\n0_3ft_FGM diff rate by season:")
for s in sorted(m['season_new'].unique()):
    mask_s = m['season_new'] == s
    nd = (d_03[mask_s].abs() > 0.01).sum()
    total = mask_s.sum()
    print(f"  {s}: {nd:>6,} / {total:>6,} ({100*nd/total:.1f}%)")

# ============================================================
# 3) TSAttempts — why does it differ 59%?
# ============================================================
print("\n" + "=" * 70)
print("3) TSAttempts")
print("=" * 70)

# Both pipelines: TSAttempts = FGA + 0.44*FTA?
ts_calc_new = m['FGA_new'].fillna(0) + 0.44 * m['FTA_new'].fillna(0)
ts_diff_new = (m['TSAttempts_new'].fillna(0) - ts_calc_new).abs()
print(f"NEW: TSAttempts vs FGA + 0.44*FTA — max diff: {ts_diff_new.max():.4f}")

ts_calc_old = m['FGA_old'].fillna(0) + 0.44 * m['FTA_old'].fillna(0)
ts_diff_old = (m['TSAttempts_old'].fillna(0) - ts_calc_old).abs()
print(f"OLD: TSAttempts vs FGA + 0.44*FTA — max diff: {ts_diff_old.max():.4f}")

# If both use the formula and FGA/FTA barely differ, why does TSAttempts differ 59%?
ts_abs = (m['TSAttempts_new'].fillna(0) - m['TSAttempts_old'].fillna(0)).abs()
print(f"\nTSAttempts abs diff distribution:")
print(ts_abs.describe())
print("\nValue counts (top 10):")
print(ts_abs.value_counts().head(10))

# Check if it's FTA-driven (0.44 * small FTA diff = 0.44)
fta_diff = (m['FTA_new'].fillna(0) - m['FTA_old'].fillna(0)).abs()
print(f"\nFTA diff > 0: {(fta_diff > 0.01).sum():,}")
print(f"TSAttempts diff where FTA matches and FGA matches:")
fga_diff = (m['FGA_new'].fillna(0) - m['FGA_old'].fillna(0)).abs()
both_match = (fta_diff < 0.01) & (fga_diff < 0.01)
print(f"  Rows where FGA+FTA match: {both_match.sum():,}")
print(f"  Of those, TSAttempts diff > 0.01: {(ts_abs[both_match] > 0.01).sum():,}")

# ============================================================
# 4) PF_DRAWN — by season
# ============================================================
print("\n" + "=" * 70)
print("4) PF_DRAWN by season")
print("=" * 70)

pf_diff = (m['PF_DRAWN_new'].fillna(0) - m['PF_DRAWN_old'].fillna(0))
print("PF_DRAWN diff (NEW - OLD):")
print(pf_diff.describe())
print("\nValue counts:")
print(pf_diff.value_counts().head(10))

print("\nBy season:")
for s in sorted(m['season_new'].unique()):
    mask_s = m['season_new'] == s
    nd = (pf_diff[mask_s].abs() > 0.01).sum()
    total = mask_s.sum()
    print(f"  {s}: {nd:>6,} / {total:>6,} ({100*nd/total:.1f}%)")

# ============================================================
# 5) POSS_OFF/POSS_DEF — are these also 2x like POSS?
# ============================================================
print("\n" + "=" * 70)
print("5) POSS_OFF / POSS_DEF individually")
print("=" * 70)

for col in ['POSS_OFF', 'POSS_DEF']:
    vn = m[f'{col}_new'].fillna(0)
    vo = m[f'{col}_old'].fillna(0)
    mask_nz = (vn > 0) & (vo > 0)
    ratio = vo[mask_nz] / vn[mask_nz]
    print(f"\n{col} ratio (OLD/NEW) where both > 0:")
    print(ratio.describe())

# ============================================================
# 6) On-court FGA/3PA barely differ but FT differs ~50%
# ============================================================
print("\n" + "=" * 70)
print("6) On-court FT columns — why do they differ so much more than FGA?")
print("=" * 70)

for col in ['OnCourt_Team_FGM', 'OnCourt_Team_FGA', 'OnCourt_Team_FT_Made',
            'OnCourt_Team_FT_Att', 'OnCourt_Team_Points']:
    cn = f'{col}_new'
    co = f'{col}_old'
    if cn in m.columns and co in m.columns:
        d = (m[cn].fillna(0) - m[co].fillna(0)).abs()
        nd = (d > 0.01).sum()
        print(f"  {col}: {nd:,} differ ({100*nd/len(m):.1f}%)")

# Check if OnCourt_Team_FT columns exist at all in OLD
for col in ['OnCourt_Team_FT_Made', 'OnCourt_Team_FT_Att']:
    co = f'{col}_old'
    if co in m.columns:
        print(f"\n  {col}_old — non-zero rows: {(m[co].fillna(0) != 0).sum():,} / {len(m):,}")
        print(f"  {col}_new — non-zero rows: {(m[f'{col}_new'].fillna(0) != 0).sum():,} / {len(m):,}")
