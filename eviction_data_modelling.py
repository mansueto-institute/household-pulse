from pathlib import Path
from etl import *

def freq_crosstab(df, col_list, weight, critical_val=1):
    pt_estimates = df.groupby(col_list, as_index=True)[[i for i in df.columns if weight in i]].agg('sum')
    pt_estimates['std_err'] = get_std_err(pt_estimates, weight)
    pt_estimates['mrgn_err'] = pt_estimates.std_err * critical_val
    return pt_estimates[[weight, 'std_err','mrgn_err']].reset_index()

def national_crosstabs(df, col_list, weights, critical_val=1):
    rv = pd.DataFrame()
    for i in col_list:
        for w in weights:
            ct = freq_crosstab(df,[i], w,critical_val)
            total = ct[w].sum()
            ct['question'] = i
            ct['proportions'] = ct.apply(lambda x: x[w]/total, axis=1)
            ct['weight'] = w
            ct = ct.rename(columns={i:'response',w:'value'})
            rv = pd.concat([rv,ct])
    return rv

def full_crosstab(df, col_list, weight, proportion_level, critical_val=1):
    df1 = df.copy()
    detail = freq_crosstab(df1, col_list, weight, critical_val)
    top = freq_crosstab(df1, proportion_level, weight, critical_val)
    rv = detail.merge(top,'left',proportion_level,suffixes=('_full','_demo'))
    rv['proportions'] = rv[weight+'_full']/rv[weight+'_demo']
    return rv

def bulk_crosstabs(df, idx_list, ct_list, q_list, select_all_questions, weight='PWEIGHT', critical_val=1):
    rv = pd.DataFrame()
    input_df = df.copy()
    for ct in ct_list:
        for q in q_list:
            full = idx_list + [ct, q]
            abstract = idx_list + [ct]
            temp = input_df[-input_df[full].isna().any(axis=1)]
            if q in select_all_questions:
                all_q = [i for i in select_all_questions if q[:-1] in i]
                temp = temp[-(temp[all_q].iloc[:,:]=='0 - not selected').all(1)]
            curr_bin = full_crosstab(temp,full,
                            weight,
                            abstract,
                            critical_val=critical_val)
            curr_bin.rename(columns={q:'q_val',ct:'ct_val'},inplace=True)
            curr_bin['ct_var'] = ct
            curr_bin['q_var'] = q
            rv = pd.concat([rv,curr_bin])
    rv['weight'] = weight
    return rv

if __name__=="__main__":

    root = Path.cwd()

    data_dir = root/"data"
    data_dir.mkdir(exist_ok=True)

    SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    ###### download housing data
    raw_housing_datafile = data_dir/"puf_housing_data_raw.csv"
    remapped_housing_datafile = data_dir/"puf_housing_data_remapped_labels.csv"
    week, mode, header, csv_cols = check_housing_file_exists(remapped_housing_datafile)

    # download crosswalk mapping tables
    question_mapping, response_mapping, county_metro_state = get_crosswalk_sheets(SERVICE_ACCOUNT_FILE)
    label_recode_dict = get_label_recode_dict(response_mapping)

    cols_file = data_dir/"list_cols.txt"
    final_cols = cols = cols_file.read_text().split(" ") if cols_file.exists() else csv_cols

    # downloads full set of weeks (from week 13 onwards) or just new weeks if housing_datafile already exists
    r = False
    while r:
        week_pad = str(week).zfill(2)
        data_str = data_url_str(week, week_pad)
        week_df = get_puf_data(data_str, week_pad)

        if week_df is None:
            r = False
        else:
            # making the process robust to variable changes in the data
            if mode == 'w':
                csv_cols = cols = final_cols = week_df.columns
            else:
                missing_csv_vars = list(set(csv_cols) - set(week_df.columns))
                print("\nWeek {} dataset, new variables: \n{}\n".format(week, list(set(week_df.columns) - set(cols))))
                print("Week {} dataset, missing variables: \n{}\n".format(week, list(set(cols) - set(week_df.columns))))
                final_cols = list(set(final_cols).intersection(set(week_df.columns)))
                cols = week_df.columns
                if missing_csv_vars:
                    week_df[missing_csv_vars] = None
                week_df = week_df[csv_cols]
            week_df.replace(label_recode_dict).to_csv(remapped_housing_datafile, mode=mode, header=header, index=False)
            week_df.to_csv(raw_housing_datafile, mode=mode, header=header, index=False)
            header, mode = (False, 'a')
            week += 1
    print("Finished downloading data")

    # save list of cols present in all weeks of data:
    cols_file.write_text(' '.join(final_cols))

    ###### generate crosstabs
    df = pd.read_csv(remapped_housing_datafile, usecols=final_cols)
    df['TOPLINE'] = 1

    question_cols = filter_non_weight_cols(final_cols)
    question_mapping_usecols = question_mapping[question_mapping['variable'].isin(question_cols)]

    select_all_questions = list(question_mapping_usecols['variable'][question_mapping_usecols['select_all_that_apply'] == '1'].unique())

    index_list = ['EST_MSA', 'WEEK']
    crosstab_list = ['TOPLINE', 'RRACE', 'EEDUC', 'INCOME']
    question_list = ['SPNDSRC1', 'SPNDSRC2', 'SPNDSRC3', 'SPNDSRC4',
                    'SPNDSRC5', 'SPNDSRC6', 'SPNDSRC7', 'SPNDSRC8',
                    'RENTCUR', 'MORTCUR', 'MORTCONF', 'EVICT', 'FORCLOSE']
    question_list2 = []
    for i in question_cols:
        if not (i in index_list+crosstab_list or 
                'WEIGHT' in i or 
                len(df[i].unique())>6):
            question_list2.append(i)

    df[select_all_questions] = df[select_all_questions].replace('-99','0 - not selected')
    df = bucketize_numeric_cols(df, question_mapping)
    df.replace(['-88','-99',-88,-99],np.nan,inplace=True)
    crosstabs = pd.concat([bulk_crosstabs(df, index_list, crosstab_list,
                                question_list2, select_all_questions,
                                weight='PWEIGHT', critical_val=1.645), 
                                bulk_crosstabs(df, index_list, crosstab_list,
                                question_list2, select_all_questions,
                                weight='HWEIGHT', critical_val=1.645)])
    crosstabs_nat = pd.concat([bulk_crosstabs(df, ['WEEK'], ['TOPLINE'],
                                question_list2, select_all_questions,
                                weight='PWEIGHT', critical_val=1.645),
                                bulk_crosstabs(df, ['WEEK'], ['TOPLINE'],
                                question_list2, select_all_questions,
                                weight='HWEIGHT', critical_val=1.645)])
    # idx one at a time? level of proportions? TODO: Fix proportion calc w/ NA
    # -99 is DNR

    crosstabs['EST_MSA'] = (crosstabs['EST_MSA'].astype(int)).astype(str)
    crosstabs = crosstabs.merge(county_metro_state[['cbsa_title','cbsa_fips']].drop_duplicates(),
                                left_on='EST_MSA',
                                right_on='cbsa_fips').iloc[:, :-1]
    crosstabs = crosstabs.merge(question_mapping[['description_recode', 'variable']],left_on='q_var', right_on='variable').iloc[:,:-1]
    crosstabs_nat['collection_dates'] = crosstabs_nat.WEEK.map(week_mapper())
    crosstabs_nat = crosstabs_nat.merge(question_mapping[['description_recode', 'variable']],left_on='q_var', right_on='variable').iloc[:,:-1]
    crosstabs['collection_dates'] = crosstabs.WEEK.map(week_mapper())
    #natl_level = national_crosstabs(df,question_list2,['PWEIGHT','HWEIGHT'],1.645)
    #natl_level = natl_level.merge(question_mapping[['description_recode', 'variable']],
    #                              left_on='question', right_on='variable')

    #crosstabs.to_csv(data_dir/'crosstabs.csv', index=False)
    #crosstabs_nat.to_csv(data_dir/'crosstabs_national.csv', index=False)

    upload_to_cloud_storage("crosstabs_output", crosstabs, "crosstabs.csv")
    upload_to_cloud_storage("crosstabs_output", crosstabs_nat, "crosstabs_national.csv")
    #upload_to_gdrive(SERVICE_ACCOUNT_FILE, data_dir/'crosstabs.csv')
    #upload_to_gdrive(SERVICE_ACCOUNT_FILE, data_dir/'crosstabs_national.csv')
    # export_to_sheets(crosstabs,'flat_file',service_account_file)
