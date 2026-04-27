import pandas as pd
new = pd.read_parquet('darko_1997_2020.parquet')
m = new[new['NbaDotComID'] != 0]
z = (m['0_3ft_FGM'].fillna(0) + m['4_9ft_FGM'].fillna(0) +
     m['10_17ft_FGM'].fillna(0) + m['18_23ft_FGM'].fillna(0))

print("FGM - zone_sum (WITHOUT 3PM):")
print((m['FGM'].fillna(0) - z).describe())
print()
print("FGM - zone_sum - 3PM (WITH 3PM):")
print((m['FGM'].fillna(0) - z - m['3PM'].fillna(0)).describe())
print()

# If zones include 3-pointers, zone_sum > (FGM - 3PM)
diff_no3 = m['FGM'].fillna(0) - m['3PM'].fillna(0) - z
print("(FGM - 3PM) - zone_sum: should be negative if 3s are in zones")
print(f"  Negative: {(diff_no3 < -0.01).sum():,}")
print(f"  Zero: {(diff_no3.abs() < 0.01).sum():,}")
print(f"  Positive: {(diff_no3 > 0.01).sum():,}")
