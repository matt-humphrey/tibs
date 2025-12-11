def get_data_transforms(changes: pl.DataFrame, file: str) -> tuple[dict, list]:
    "Build rename dict and recode expressions from the changes specified in the spreadsheet."
    fc = changes.filter(pl.col('file').eq(file))
    r = fc.filter(pl.col('new_var_name').is_not_null())
    renames = dict(zip(r['old_var_name'], r['new_var_name']))
    rc = fc.group_by('recode').agg(pl.col('old_var_name')).filter(pl.col('recode').is_not_null())
    exprs = [pl.col(row['old_var_name']).replace(ast.literal_eval(row['recode'])) 
             for row in rc.iter_rows(named=True)]
    return renames, exprs
def harmonise_data(df: pl.DataFrame, changes: pl.DataFrame, file: str) -> pl.DataFrame:
    renames, exprs = get_data_transforms(changes, file)
    return df.with_columns(*exprs).rename(renames)
def harmonise_metadata(m: pl.DataFrame, changes: pl.DataFrame) -> pl.DataFrame:
    """Apply variable name, label, and field value changes to metadata."""
    return m.join(changes, left_on="Variable", right_on="old_var_name", how="left").with_columns(
        pl.coalesce("new_var_name", "Variable").alias("Variable"),
        pl.coalesce("new_var_label", "Label").alias("Label"),
        pl.coalesce("new_field_values", "Field Values").alias("Field Values"),
    ).select(m.columns)
def test_unchanged_columns(old_df: pl.DataFrame, new_df: pl.DataFrame, renames: dict) -> bool:
    """Verify columns not recoded/renamed are identical between old and new dataframes."""
    changed_cols = set(renames.keys())
    unchanged_cols = [c for c in old_df.columns if c not in changed_cols]
    
    for col in unchanged_cols:
        if not old_df[col].equals(new_df[col]):
            print(f"Mismatch in column: {col}")
            return False
    print(f"All {len(unchanged_cols)} unchanged columns are identical.")
    return True
def harmonise(changes: pl.DataFrame):
    "Complete the harmonisation project, writing all changes specified in the `changes.xlsx` file to the respective SPSS files in `data/input`"
    fc = changes.filter(pl.col("new_var_name").is_not_null() | pl.col("new_var_label").is_not_null() | pl.col("new_field_values").is_not_null() | pl.col("recode").is_not_null())
    for f in fc['file'].unique().to_list():
        df, m = bk.read_sav(INPUT/f)
        renames, exprs = get_data_transforms(fc, f)
        dfx = harmonise_data(df, fc, f)
        mx = harmonise_metadata(m, fc)
        assert test_unchanged_columns(df, dfx, renames)
        bk.write_sav(OUTPUT/f, dfx, mx)