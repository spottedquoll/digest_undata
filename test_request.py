import os
import itertools
import requests
import time
import pandas as pd
import zipfile

# Paths
work_dir = os.environ['work_dir']
save_dir = work_dir + 'scraped/'

if not os.path.isdir(save_dir):
    os.mkdir(save_dir)

# Options
year_rng = range(2015, 2018)
country_rng = range(30, 32)
table_rng = [102, 201, 401, 501]

iter_options = list(itertools.product(year_rng, country_rng, table_rng))

# URL components
base_url_snippet = 'http://data.un.org/Handlers/DownloadHandler.ashx?'
end_url_snippet = '&Format=csv&c=2,3,4,6,7,8,9,10,11,12,13,14&s=_cr_engNameOrderBy:asc,fiscal_year:desc,_grIt_code:asc'

# Loop over options and perform requests
for opt in iter_options:

    # Unpack options
    year = opt[0]
    country_code = opt[1]
    table_id = opt[2]

    result_fname = save_dir + 'unoc_c' + str(country_code) + '_t' + str(table_id) + '_' + str(year)

    # Download if file does not yet exist
    if not os.path.isfile(result_fname + '.xlsx'):

        # Construct request url
        data_filter = ('DataFilter=group_code:' + str(table_id) + ';country_code:' + str(country_code)
                       + ';fiscal_year:' + str(year) + '&DataMartId=SNA')
        csv_url = base_url_snippet + data_filter + end_url_snippet

        # Perform request
        req = requests.get(csv_url, allow_redirects=True)
        if req.status_code == 200:  # check is response is OK

            url_content = req.content

            # Unpack the response
            dl_zip_file = open(result_fname + '.zip', 'wb')
            dl_zip_file.write(url_content)
            dl_zip_file.close()

            # Check the data exists (is more than a header)
            if 'Content-length' in req.headers and int(req.headers['Content-length']) > 296:

                # Unzip the archive
                zf = zipfile.ZipFile(result_fname + '.zip')
                zip_contents = zipfile.ZipFile.namelist(zf)

                assert len(zip_contents) == 1  # There should be only one file
                raw_csv_name = zip_contents[0]

                f = zf.open(raw_csv_name)
                content = f.read()
                f = open(result_fname + '.csv', 'wb')
                f.write(content)
                f.close()

                data = pd.read_csv(result_fname + '.csv')
                data.to_excel(result_fname + '.xlsx', index=False)

                assert data.shape[1] < 90000  # Limit of UN request

                # Delete the working files if the data exists, keep the empty zips so files are not downloaded again
                os.remove(result_fname + '.zip')
                os.remove(result_fname + '.csv')
        else:
            print('Request failed (' + str(req.status_code) + ') for ' + data_filter)

    # Wait between requests so as to not abuse the UN API
    time.sleep(10)
