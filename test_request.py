import os
import itertools
import requests
import time
import pandas as pd
from tinydb import TinyDB, Query
import zipfile

# Paths
work_dir = os.environ['work_dir']
save_dir = work_dir + 'scraped/'

if not os.path.isdir(save_dir):
    os.mkdir(save_dir)

# URL components
base_url_snippet = 'http://data.un.org/Handlers/DownloadHandler.ashx?'
end_url_snippet = '&Format=csv&c=2,3,4,6,7,8,9,10,11,12,13,14&s=_cr_engNameOrderBy:asc,fiscal_year:desc,_grIt_code:asc'

# Retrieve list of UN 2-digit country codes
country_legend = pd.read_excel(os.environ['legend_dir'] + 'RootRegionLegend.xlsx', sheet_name='RootCountryLegend')
un_country_codes = country_legend['UNCode'].values

# Options
year_rng = range(2015, 2018)
country_rng = un_country_codes
table_rng = [101]

iter_options = list(itertools.product(year_rng, country_rng, table_rng))

# Create a store to remember requests
db = TinyDB(work_dir + 'request_store.json')
#db.purge_tables()

# Loop over options and perform requests
for opt in iter_options:

    # Unpack options
    year = int(opt[0])
    country_code = int(opt[1])
    table_id = opt[2]

    db_table = db.table('tbl' + str(table_id))

    # Download if file does not yet exist
    previous_request = db_table.search((Query()['country'] == country_code) & (Query()['year'] == year))

    if not previous_request:

        # Construct request url
        data_filter = ('DataFilter=group_code:' + str(table_id) + ';country_code:' + str(country_code)
                       + ';fiscal_year:' + str(year) + '&DataMartId=SNA')
        csv_url = base_url_snippet + data_filter + end_url_snippet

        # Add table subtype to query...

        # Perform request
        req = requests.get(csv_url, allow_redirects=True)
        if req.status_code == 200:  # check is response is OK

            url_content = req.content

            # Unpack the response
            result_fname = save_dir + 'unoc_c' + str(country_code) + '_t' + str(table_id) + '_' + str(year)
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
                db_table.insert({'country': country_code, 'year': year, 'result': 1})

            else:
                # Store request failure
                db_table.insert({'country': country_code, 'year': year, 'result': 0})

            # Delete the zips
            os.remove(result_fname + '.zip')

        else:
            print('Request failed (' + str(req.status_code) + ') for ' + data_filter)

    # Wait between requests so as to not abuse the UN API
    time.sleep(10)
