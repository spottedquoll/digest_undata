import os
import requests
import time
import pandas as pd
from tinydb import TinyDB, Query
import zipfile

print('Running UNSTAT scraper')

# Paths
work_dir = os.environ['work_dir']
save_dir = work_dir + 'scraped/'

if not os.path.isdir(save_dir):
    os.mkdir(save_dir)

# URL components
base_url_snippet = 'http://data.un.org/Handlers/DownloadHandler.ashx?&Format=csv&'
end_url_snippet = '&s=_cr_engNameOrderBy:asc,fiscal_year:desc,_grIt_code:asc'

# Retrieve list of UN 2-digit country codes
print('Getting UN country codes')

country_legend = pd.read_excel(os.environ['legend_dir'] + 'RootRegionLegend.xlsx', sheet_name='RootCountryLegend')
un_country_codes = country_legend['UNCode'].values

# Timeseries options
year_start = 1990
year_end = 2018
year_rng = range(year_start, year_end)
all_year_strings = [str(x) for x in list(year_rng)]

# UN Table options
table_optons = [{'tbl_id': 101, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tbl_id': 102, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13,14'}
                , {'tbl_id': 103, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tbl_id': 201, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 202, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13,14'}
                , {'tble_id': 203, 'col_specs': 'c=2,3,5,6,8,9,10,11,12,13,14,15'}
                , {'tble_id': 204, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 205, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13,14'}
                , {'tble_id': 206, 'col_specs': 'c=2,3,5,6,8,9,10,11,12,13,14,15'}
                , {'tble_id': 301, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 302, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 401, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 402, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 403, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 404, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 405, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 406, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 407, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 408, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 409, 'col_specs': 'c=2,3,4,6,7,8,9,10,11,12,13'}
                , {'tble_id': 501, 'col_specs': 'c=2,3,5,6,8,9,10,11,12,13,14,15'}
                , {'tble_id': 502, 'col_specs': 'c=2,3,5,6,8,9,10,11,12,13,14,15'}
                ]

# Create a store to remember requests
print('Creating tinyDB')

db = TinyDB(work_dir + 'request_store.json')
#db.drop_tables()

# Loop over options and perform requests
print('Scraping...')

for un_table in table_optons:

    un_table_id = un_table['tbl_id']

    print('Table ' + str(un_table_id))

    # Create db table to match un table
    db_table = db.table('tbl' + str(un_table_id))

    # Output directory
    scraped_dir = save_dir + 'table' + str(un_table_id) + '/'
    if not os.path.isdir(scraped_dir):
        os.mkdir(scraped_dir)

    for country_code in un_country_codes:

        # 3 digit country name
        country_acronym = country_legend[country_legend['UNCode'] == country_code]['Root country abbreviation'].values[0]

        # Download if file does not yet exist
        previous_request = db_table.search(Query()['country'] == country_code)

        if not previous_request:

            print(' Downloading ' + country_acronym)

            # Wait between requests so as to not abuse the UN API
            time.sleep(10)

            # Construct request url
            csv_url = (base_url_snippet + 'DataFilter=group_code:' + str(un_table_id)
                       + ';country_code:' + str(country_code) + ';fiscal_year:' + ','.join(all_year_strings)
                       + '&DataMartId=SNA&Format=csv&' + un_table['col_specs'] + end_url_snippet)

            # Make request
            req = requests.get(csv_url, allow_redirects=True)

            status_code = req.status_code
            if status_code == 200:  # response is OK

                result_fname = (scraped_dir + 'UNOC_' + country_acronym + '_' + str(country_code)
                                + '_table' + str(un_table_id) + '_' + str(year_start) + '-' + str(year_end))

                # Unpack the response
                url_content = req.content
                dl_zip_file = open(result_fname + '.zip', 'wb')
                dl_zip_file.write(url_content)
                dl_zip_file.close()

                # Check the data exists (is more than a header)
                if 'Content-length' in req.headers and int(req.headers['Content-length']) > 296:

                    # Unzip the archive
                    zf = zipfile.ZipFile(result_fname + '.zip')
                    zip_contents = zipfile.ZipFile.namelist(zf)

                    assert len(zip_contents) == 1  # There should be only one file

                    # Save as csv
                    raw_csv_name = zip_contents[0]

                    f = zf.open(raw_csv_name)
                    content = f.read()
                    f = open(result_fname + '.csv', 'wb')
                    f.write(content)
                    f.close()

                    # Check for dataloss
                    # Request limit is 100K, if size is close to this there may be dataloss
                    data = pd.read_csv(result_fname + '.csv')
                    assert data.shape[1] < 90000

                    # Store request success
                    db_table.insert({'country': int(country_code), 'success': 1, 'response': status_code})

                else:
                    # Store request failure
                    db_table.insert({'country': int(country_code), 'success': 0, 'response': status_code})

                # Delete the zips
                os.remove(result_fname + '.zip')

            else:
                print('Request failed (' + str(status_code) + ') for table' + str(un_table_id) + 'country'
                      + str(country_code))

    any_successful_requests = db_table.search(Query()['success'] == 1)
    assert any_successful_requests is not []

    print('.')

print('All finished')