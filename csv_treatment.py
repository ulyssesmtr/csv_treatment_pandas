import pandas as pd


def process(exp_path, imp_path, res_path):
    dict_result = {}  # Final result, to be used to create the dataframe
    net_dict = {}  # Aux dict to store data during iterations
    exp_dict = {}  # Aux dict to store data during iterations
    imp_dict = {}  # Aux dict to store data during iterations

    df_exp = pd.read_csv(exp_path, delimiter=';')  # exp dataframe
    df_imp = pd.read_csv(imp_path, delimiter=';')  # imp dataframe
    state_list = set(df_exp['SG_UF_NCM']).union(set(df_imp['SG_UF_NCM']))  # Gets the list of states

    for state in state_list:
        total_exp_dict = {}
        total_imp_dict = {}
        total_net_dict = {}
        df_exp_state_filtered = (df_exp.loc[df_exp['SG_UF_NCM'] == state])  # Filters by state
        df_imp_state_filtered = (df_imp.loc[df_imp['SG_UF_NCM'] == state])  # Filters by state

        for month in range(1, 13):
            df_exp_month_filtered = df_exp_state_filtered.loc[df_exp_state_filtered['CO_MES'] == month]  # Filters by month
            df_imp_month_filtered = df_imp_state_filtered.loc[df_imp_state_filtered['CO_MES'] == month]  # Filters by month
            ncm_list_exp_current_month = set(df_exp_month_filtered['CO_NCM'])  # Gets the list of NCMS in that specific month
            ncm_list_imp_current_month = set(df_imp_month_filtered['CO_NCM'])  # Gets the list of NCMS in that specific month
            ncm_list_current_moth = ncm_list_exp_current_month.union(ncm_list_imp_current_month) # All the ncms for that month

            for ncm in ncm_list_current_moth:
                df_exp_ncm_filtered = df_exp_month_filtered.loc[df_exp_month_filtered['CO_NCM'] == ncm]  # Filters by ncm
                df_imp_ncm_filtered = df_imp_month_filtered.loc[df_imp_month_filtered['CO_NCM'] == ncm]  # Filters by ncm
                vl_fob_exp = df_exp_ncm_filtered['VL_FOB'].sum()  # Gets the sum of all the VL_FOB for that NCM
                vl_fob_imp = df_imp_ncm_filtered['VL_FOB'].sum()  # Gets the sum of all the VL_FOB for that NCM
                net = vl_fob_exp - vl_fob_imp
                exp_dict[ncm] = vl_fob_exp
                imp_dict[ncm] = vl_fob_imp
                net_dict[ncm] = net
                if ncm in total_exp_dict:
                    total_exp_dict[ncm] += vl_fob_exp
                    total_imp_dict[ncm] += vl_fob_imp
                    total_net_dict[ncm] += net
                else:
                    total_exp_dict[ncm] = vl_fob_exp
                    total_imp_dict[ncm] = vl_fob_imp
                    total_net_dict[ncm] = net

            dict_result[f'{month}_exp'] = exp_dict.copy()
            dict_result[f'{month}_imp'] = imp_dict.copy()
            dict_result[f'{month}_net'] = net_dict.copy()
            net_dict.clear()
            imp_dict.clear()
            exp_dict.clear()

        dict_result['2020_exp'] = total_exp_dict.copy()
        dict_result['2020_imp'] = total_imp_dict.copy()
        dict_result['2020_net'] = total_net_dict.copy()

        result_df = pd.DataFrame(dict_result)  # Creates the dataframe
        result_df = result_df.reindex(sorted(result_df.columns, key=lambda x: int(x.split('_')[0])), axis=1)  # Sort columns
        result_df.index.name = 'NCM'
        result_df.to_csv(f'{res_path}\\{state}.csv')
        dict_result.clear()


path_exp = 'D:\Turim\dados_2020\EXP_2020.csv'
path_imp = 'D:\Turim\dados_2020\IMP_2020.csv'
path_result = 'D:\Turim\dados_estados'

process(path_exp, path_imp, path_result)

