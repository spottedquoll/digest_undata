import os
import pandas as pd

print('Joining OC country data')

# Paths
work_dir = os.environ['work_dir']
save_dir = work_dir + 'scraped/'
joined_dir = work_dir + 'joined/'

if not os.path.isdir(joined_dir):
    os.mkdir(joined_dir)

# Retrieve list of UN 2-digit country codes
print('Getting UN country codes')

country_legend = pd.read_excel(os.environ['legend_dir'] + 'RootRegionLegend.xlsx', sheet_name='RootCountryLegend')
un_country_codes = country_legend['UNCode'].values

# Timeseries options
year_start = 1990
year_end = 2018

# Tables to join
table_codes = [201]  #101, , 203, 302, 401]

# Apply change to columns


def fix_group_code(row):
    return str.replace(str(row['group_code']), '.', '0')


def fix_group_name(row):
    return 'Table ' + row['group_code'].replace('0', '.') + ' ' + row['group_name']


def fix_group_name_va(row):
    if not isinstance(row['group_name'], str):
        group_name = 'Industries'
    else:
        group_name = row['group_name']
    return 'Table ' + row['group_code'].replace('0', '.') + ' Value added by ' + group_name


print('Joining')

for un_table in table_codes:

    un_table_id = un_table
    print('Table ' + str(un_table_id))

    scraped_dir = save_dir + 'table' + str(un_table_id) + '/'
    df_joined = pd.DataFrame()

    for country_code in un_country_codes:

        # 3 digit country name
        country_acronym = country_legend[country_legend['UNCode'] == country_code]['Root country abbreviation'].values[0]

        result_fname = (scraped_dir + 'UNOC_' + country_acronym + '_' + str(country_code)
                        + '_table' + str(un_table_id) + '_' + str(year_start) + '-' + str(year_end) + '.csv')

        if os.path.isfile(result_fname):
            data = pd.read_csv(result_fname)

            # Drop note rows at the bottom
            bottom_note_idx = data.index[data['Country or Area'] == 'footnote_SeqID'].tolist()
            if bottom_note_idx:
                cut_idx = data.shape[0] - bottom_note_idx[0]
                data.drop(data.tail(cut_idx).index, inplace=True)

            # Prepend country code column
            data.insert(0, 'country_code', str(country_code), True)

            # Rename columns
            data = data.rename(columns={'Country or Area': 'country_name', 'Item': 'item_name'
                                        , 'SNA93 Table Code': 'group_code', 'Sub Group': 'group_name'
                                        , 'SNA93 Item Code': 'sna93_item_code', 'Year': 'fiscal_year'
                                        , 'Series': 'series_number', 'Currency': 'currency_name'
                                        , 'Value Footnotes': 'footnote_text', 'SNA System': 'base_year'
                                        , 'Value': 'sna_value'
                                        }
                               , errors='raise')

            # Fix colummns
            if un_table in [101, 201]:

                data['group_code'] = data.apply(fix_group_code, axis=1)

                if un_table is 101:
                    data['group_name'] = data.apply(fix_group_name, axis=1)
                elif un_table is 201:
                    data['group_name'] = data.apply(fix_group_name_va, axis=1)

                data.insert(4, 'item_code', 99)
                col = data.pop('sna93_item_code')
                data.insert(5, col.name, col)
                data.insert(7, 'sub_item_code', 0)
                data.insert(8, 'sub_item_name', '')
                data.insert(11, 'sub_series_number', 0)
                col = data.pop('currency_name')
                data.insert(16, col.name, col)
                data = data.drop(['Fiscal Year Type'], axis=1)  # Remove columns

            # Do join
            if df_joined.shape[0] != 0:
                assert df_joined.shape[1] == data.shape[1]

            df_joined = pd.concat([df_joined, data], ignore_index=True)

    new_fname = joined_dir + 'Table ' + str(un_table_id) + '.txt'
    df_joined.to_csv(new_fname, header=True, index=False, sep='\t')

print('Done')