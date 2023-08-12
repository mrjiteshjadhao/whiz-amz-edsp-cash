"""Amazon-cash-management-check which uses db_connector for site login creds instead of Excel file"""

from whizzbox import config, toolkit
from dotenv import load_dotenv
from whizzbox import s3_connector as s3c
from selenium.webdriver.common.by import By  # to use By parameter in find_elements method
from selenium.webdriver.common.keys import Keys  # to use Keys in send_keys method
from selenium.webdriver.support.ui import WebDriverWait  # to wait till certain elements were located
from selenium.webdriver.support import expected_conditions as EC  # to use expected conditions with WebDriverWait
import pandas as pd  # to work with dataframe and CSVs
import numpy as np  # to perform numerical calculations
import time  # to get current time
import datetime  # to get the current date
import os  # to work with files and folders
from whizzbox import db_connector, site_login  #
import pytz
import warnings
from bs4 import BeautifulSoup as bs
import re

warnings.filterwarnings("ignore")

PROJECT_NAME = 'whiz-amz-edsp-cash'
HEADLESS = True
TEST = False  # if it's a test, email recipients are limited
SAMPLE = False  # if it's a sample, no. of sites are limited
SEND_EMAIL = True
SEND_FAIL_EMAIL = True

def get_the_driver_recon(driver, page, site_code):
    recon_page_url = page + "driverreconciliation?stationCode=" + str(site_code)[0:4]
    driver.get(recon_page_url)

    # frame id for driver recon
    driver_frame_id = 'DriverReconciliationMeridianBlock'
    driver_table_frame = driver.find_element(By.ID, driver_frame_id)
    driver.switch_to.frame(driver_table_frame)  # switch to the iframe which contains the driver recon table
    WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Select Driver']")))
    select_driver = driver.find_element(By.XPATH, "//input[@placeholder='Select Driver']")
    select_driver.click()
    select_driver.send_keys('All Drivers')
    select_driver.send_keys(Keys.RETURN)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table")))
    table = driver.find_element(By.XPATH, "//table")
    page_source = driver.page_source
    doc = bs(page_source, 'html.parser')
    time.sleep(2)
    table_df = pd.read_html(table.get_attribute('outerHTML'))[0]

    df = pd.DataFrame()
    df['Name'] = table_df[table_df.columns[0]]
    df['ID'] = table_df[table_df.columns[1]]
    df['Provider Name'] = table_df[table_df.columns[2]]
    df['Type'] = table_df[table_df.columns[3]]
    df['Expected'] = table_df[table_df.columns[4]]
    df['Undebriefed MPOS'] = table_df[table_df.columns[5]]
    df['Undebriefed CASH'] = table_df[table_df.columns[6]]
    df['Variance'] = table_df[table_df.columns[7]]
    df['Running Balance'] = table_df[table_df.columns[8]]
    df['Pending Recon'] = table_df[table_df.columns[9]]
    df['Site Code'] = site_code

    rows = doc.find_all('p')
    rows = str(rows)
    clean = re.compile('<*?>')
    clean2 = (re.sub(clean, '', rows))
    clean2 = clean2.replace(',', '\n')
    df1 = pd.DataFrame([x.split(' ') for x in clean2.split('\n')])
    df1.loc[0][3] = df1.loc[0][2]
    df1.loc[0][2] = df1.loc[0][1]
    df1.loc[0][1] = df1.loc[0][0]
    str_cols = df1.columns
    df1[str_cols] = df1[str_cols].replace('<p', '', regex=True)
    # df1[str_cols] = df1[str_cols].replace('p', '', regex=True)
    df1[str_cols] = df1[str_cols].replace('</p', '', regex=True)
    df1[str_cols] = df1[str_cols].replace('"', '', regex=True)
    df1[str_cols] = df1[str_cols].replace('mdn-text=', '', regex=True)
    df1[str_cols] = df1[str_cols].replace('₹', '', regex=True)
    # df1[str_cols] = df1[str_cols].replace('[', '', regex=True)
    df1[str_cols] = df1[str_cols].replace(']', '', regex=True)
    df1[str_cols] = df1[str_cols].replace('', np.nan, regex=True)
    df1 = df1[[3, 5]]
    df1['Site Code'] = site_code
    df1 = df1.rename(columns={3: 'Category', 5: 'Running Balance'})

    return df, df1


def get_the_bank_deposit(driver, page, site_code):
    bank_page_url = page + "bankdeposits?stationCode=" + str(site_code)[0:4]
    driver.get(bank_page_url)
    bank_frame_id = 'BankDepositsMeridianBlock'
    time.sleep(2)
    bank_table_frame = driver.find_element(By.ID, bank_frame_id)
    driver.switch_to.frame(bank_table_frame)  # switch to the iframe which contains the deposit table
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, "//table")))
    table = driver.find_element(By.XPATH, "//table")
    time.sleep(2)
    table_df = pd.read_html(table.get_attribute('outerHTML'))[0]

    df = pd.DataFrame()
    df['Code'] = table_df[table_df.columns[0]]
    df['Created by'] = table_df[table_df.columns[1]]
    df['Creation date'] = table_df[table_df.columns[2]]
    df['Submitted by'] = table_df[table_df.columns[3]]
    df['Last Updated on'] = table_df[table_df.columns[4]]
    df['Status'] = table_df[table_df.columns[5]]
    df['Expected Amount'] = table_df[table_df.columns[6]]
    df['Actual Amount'] = table_df[table_df.columns[7]]
    df['Variance Amount'] = table_df[table_df.columns[8]]
    df['Variance Reason'] = table_df[table_df.columns[9]]
    df['Site Code'] = site_code

    return df


def replace_currency_str(df):
    str_cols = df.columns
    df[str_cols] = df[str_cols].replace('₹ ', '', regex=True)
    return df


def merge_recon_w_category(df, df1):
    df['Unique Id'] = (df['Running Balance'].astype(str) + '/' + df['Site Code'].astype(str))
    df1['Unique Id'] = (df1['Running Balance'].astype(str) + '/' + df1['Site Code'].astype(str))
    df = pd.merge(df, df1[['Unique Id', 'Category']], on='Unique Id', how='left')
    df = df.drop(columns=['Unique Id'])
    return df


def total_due_amount(df):
    con = df['Category'].str.contains('excess')
    con2 = df['Category'].str.contains('short')
    con3 = df['Category'].isna()
    df['Cash Submitted Category'] = np.where(con3, 'None',
                                             np.where(con2, 'Short cash submitted',
                                                      np.where(con, 'Excess cash submitted', 'None')))
    total = []
    for n in range(len(df)):
        if df['Cash Submitted Category'][n] == 'Excess cash submitted':
            total.append(df['Pending Recon'][n] - df['Running Balance'][n])
        elif df['Cash Submitted Category'][n] == 'Short cash submitted':
            total.append(df['Pending Recon'][n] + df['Running Balance'][n])
        elif df['Cash Submitted Category'][n] == 'None':
            total.append(df['Pending Recon'][n])
        else:
            total.append(df['Pending Recon'][n])
    df['Total Amount Due'] = total
    df = df[['Name', 'ID', 'Provider Name', 'Type', 'Expected', 'Undebriefed MPOS',
             'Undebriefed CASH', 'Variance', 'Running Balance',
             'Category', 'Cash Submitted Category', 'Pending Recon', 'Total Amount Due', 'Site Code']]
    df = merger_df_site_details(df1=df, df2=login_creds_df, join_type='left',
                                cca_time=False)
    df['Date'] = today
    df['Time'] = hour

    df = df.drop(columns=['Category'])

    return df


def format_date_column(df, date_column_name):
    df[f'{date_column_name}_new'] = df[date_column_name].str.split(' ', expand=True)[0]
    df[f'{date_column_name}_new'] = pd.to_datetime(df[f'{date_column_name}_new'])
    return df


def filter_df_keywords(df, column_name, keyword):
    new_df = df[df[column_name].str.contains(keyword)]
    new_df = new_df.reset_index(drop=True)
    return new_df


def pivoted_df_single_value(df, index_columns, value_column, agg_type):
    new_df = pd.pivot_table(df, index=index_columns, values=value_column, aggfunc=agg_type)
    new_df = new_df.reset_index()
    return new_df


def filter_df_doesnot_equal_to_value(df, column_name, value):
    new_df = df[df[column_name] != value]
    new_df = new_df.reset_index(drop=True)
    return new_df


def filter_df_equal_to_value(df, column_name, value):
    new_df = df[df[column_name] == value]
    new_df = new_df.reset_index(drop=True)
    return new_df


def remit_not_created_amazon(df, df2):
    df_codes = list(df2['Site Code'].unique())

    # Remove the ones where scraping has failed
    for i in range(len(failed)):
        df_codes.remove(failed[i])

    # Check if remittances have been created
    remittance_created_list = []
    for i in range(len(df_codes)):
        main_df = df[df['Site Code'] == df_codes[i]]
        main_df = main_df[main_df['Creation date_new'] == str(today)]
        df_len = len(main_df)

        if df_len > 0:
            remittance_created_list.append('Yes')
        else:
            remittance_created_list.append('No')

    df_main = pd.DataFrame()
    df_main['Site Code'] = df_codes
    df_main['Date'] = today
    df_main['Time'] = hour
    df_main['Remittance Created'] = remittance_created_list

    df_main = df_main[df_main['Remittance Created'] == 'No']
    df_main = df_main.reset_index(drop=True)

    return df_main


def convert_to_float_dtype(df, df_type):
    if df_type:
        list_of_columns = ['Expected', 'Undebriefed MPOS', 'Undebriefed CASH', 'Variance', 'Running Balance',
                           'Pending Recon']
    else:
        list_of_columns = ['Expected Amount', 'Actual Amount', 'Variance Amount']

    df[list_of_columns] = round(df[list_of_columns].astype(float), 2)
    return df


def merger_df_site_details(df1, df2, join_type, cca_time):
    if cca_time:
        list_of_columns = ['Site Code', 'Client', 'OM', 'RM', 'CMS Time']
    else:
        list_of_columns = ['Site Code', 'Client', 'OM', 'RM']

    new_df = pd.merge(df1, df2[list_of_columns], on=['Site Code'], how=join_type)
    return new_df


def create_remit_df(df, older_remit):
    if older_remit:
        new_column_name = 'Amount Not Deposited - Older'
    else:
        new_column_name = 'Amount Not Deposited - ' + str(today)
    if not df.empty:
        df = pd.pivot_table(df,
                            index=['Date', 'Time', 'Site Code', 'Client', 'RM', 'OM'],
                            values=['Expected Amount'],
                            aggfunc='sum',
                            fill_value=0)

        df = df.rename(columns={'Expected Amount': new_column_name})

        df = df.reset_index()

    else:
        df = pd.DataFrame()
        df['Date'] = today
        df['Time'] = hour
        df['Site Code'] = ''
        df['Client'] = ''
        df['RM'] = ''
        df['OM'] = ''
        df[new_column_name] = ''
    return df


def create_summary(df1, df2, df3, df4):
    cms = login_creds_df[['Site Code', 'CMS Time']]
    column_list = ['Date', 'Time', 'Site Code', 'Client', 'RM', 'OM']

    main_df = pd.merge(df1, df2, on=column_list, how='outer')
    main_df = pd.merge(main_df, df3, on=column_list, how='outer')
    main_df = pd.merge(main_df, df4, on=column_list, how='outer')

    main_df['Remittance Created'] = main_df['Remittance Created'].fillna('Yes')
    main_df = pd.merge(main_df, cms, on='Site Code', how='left')
    main_df = main_df.fillna(0)
    main_df = main_df.rename(columns={'Total Amount Due': 'AMZN Only - Pending Driver Recon',
                                      'Remittance Created': 'AMZN Only - Remittance Created (Y/N)'})
    main_df['Overall Cash Pendency - Older'] = (main_df['Amount Not Deposited - Older'] +
                                                main_df['AMZN Only - Pending Driver Recon'])
    main_df['Overall Cash Pendency - ' + str(today)] = main_df['Amount Not Deposited - ' + str(today)]
    main_df['Deposited To Company Account'] = ' '
    main_df['Already Recovered'] = ' '
    main_df['Pending Recovery Shared'] = ' '
    main_df['Legal Dispute'] = ' '
    main_df['Remarks'] = ' '
    main_df = main_df[['OM', 'RM', 'Site Code', 'Client', 'CMS Time',
                       'Amount Not Deposited - Older',
                       'Amount Not Deposited - ' + str(today),
                       'AMZN Only - Pending Driver Recon',
                       'AMZN Only - Remittance Created (Y/N)',
                       'Overall Cash Pendency - Older',
                       'Overall Cash Pendency - ' + str(today),
                       'Deposited To Company Account',
                       'Already Recovered',
                       'Pending Recovery Shared',
                       'Legal Dispute',
                       'Remarks']]
    main_df[['Amount Not Deposited - Older',
             'Amount Not Deposited - ' + str(today),
             'AMZN Only - Pending Driver Recon',
             'Overall Cash Pendency - Older',
             'Overall Cash Pendency - ' + str(today)]] = round(main_df[['Amount Not Deposited - Older',
                                                                        'Amount Not Deposited - ' + str(today),
                                                                        'AMZN Only - Pending Driver Recon',
                                                                        'Overall Cash Pendency - Older',
                                                                        'Overall Cash Pendency - ' + str(
                                                                            today)]].astype(float), 2)
    return main_df


def file_saving_path(name, path, client_type):
    return path + '/' + name + '.xlsx' if client_type else None


def failed_site_message(df, client_name):
    if df.empty:
        text = 'Automated checks are successful for all stations.'
    else:
        if not df[df['Client'] == client_name].empty:
            failed_stations = df[df['Client'] == client_name]['Site Code'].unique()
            failed_stns_str = str(
                str(failed_stations).replace('\n', '').replace(' ', ',').replace('[', '').replace(']', ''))
            text = f"""Automated checks for these <b>{len(failed_stations)}</b> \
			station(s) have failed - {failed_stns_str}.\
			Please login to the Amazon Station Command Center and conduct a manual check on their cash management."""
        else:
            text = 'Automated checks are successful for all stations.'
    return text


def pending_recon_message(df, client_name):
    if not df[df['Client'] == client_name].empty:
        recon_statons_amount = round(df[df['Client'] == client_name]['Total Amount Due'].sum(), 2)
        recon_amount_str = str(recon_statons_amount)
        recon_statons = df[df['Client'] == client_name]['Site Code'].unique()
        recon_statons_str = str(
            str(recon_statons).replace('\n', '').replace(' ', ',').replace('[', '').replace(']', ''))

        text = f"""There is a total of <b>₹ {recon_amount_str}/-</b> \
		due to us from associates at <b>{len(recon_statons)}</b> station(s) - {recon_statons_str}."""
    else:
        text = 'There are no pending dues from any associate.'
    return text



def short_excess_message(df, client_name):
    df = df[df['submitted_short_excess'] != 0]
    df = df.reset_index(drop=True)
    if not df[df['client_name'] == client_name].empty:
        recon_statons_amount = (0 - round(df[df['client_name'] == client_name]['submitted_short_excess'].sum(), 2))
        recon_amount_str = str(recon_statons_amount)
        recon_statons = df[df['client_name'] == client_name]['station'].unique()
        # recon_statons_str = str(
        #     str(recon_statons).replace('\n', '').replace(' ', ',').replace('[', '').replace(']', ''))

        text = f"""There is a total of <b>₹ {recon_amount_str}/-</b> \
        due to us from associates at <b>{len(recon_statons)}</b> station(s)."""

    else:
        text = 'There are no pending dues from any associate.'
    return text


def uncreated_message(df, client_name):
    if df.empty:
        text = 'Remittances were created for all stations today.'
    else:
        if not df[df['Client'] == client_name].empty:
            uncreated_stations_amount = round(df[df['Client'] == client_name]['Expected Amount'].sum(), 2)
            uncreated_amount_str = str(uncreated_stations_amount)
            uncreated_stations = df[df['Client'] == client_name]['Site Code'].unique()
            uncreated_stations_str = str(
                str(uncreated_stations).replace('\n', '').replace(' ', ',').replace('[', '').replace(']', ''))
            text = f"""Remittances amounting to approx. <b>₹ {uncreated_amount_str}/-</b> \
            were not created today for <b>{len(uncreated_stations)}</b> station(s) - {uncreated_stations_str}."""

        else:
            text = 'Remittances were created for all stations today.'
    return text


def remittance_message(df):
    if df.empty:
        text = "Excluding today's date, no remittances are pending for deposit."
    else:
        if not df[df['Amount Not Deposited - Older'] != 0].empty:
            remit_statons_amount = round(
                df[df['Amount Not Deposited - Older'] != 0]['Amount Not Deposited - Older'].sum(), 2)
            remit_amount_str = str(remit_statons_amount)
            remit_statons = df[df['Amount Not Deposited - Older'] != 0]['Site Code'].unique()
            remit_statons_str = str(
                str(remit_statons).replace('\n', '').replace(' ', ',').replace('[', '').replace(']', ''))

            text = f"""Excluding today's date, remittances of <b>₹ {remit_amount_str}/-</b> \
            have not been deposited yet from <b>{len(remit_statons)}</b> station(s) - {remit_statons_str}."""
        else:
            text = "Excluding today's date, no remittances are pending for deposit."
    return text


def loss_oor_message(df, df_type):
    if df_type =='loss':
        short_text = 'Till date there is no potential loss.'
        short_text_default = ', there is a potential loss'
        value_column = 'value'
    else:
        short_text = 'All orders were returned to station.'
        short_text_default = ', there are orders - out on road with no RTS'
        value_column = 'value(as per scc)'

    if df.empty:
        text = {short_text}
    else:
        df[value_column] = df[value_column].fillna(0)
        statons_amount = round(
            df[df[value_column] != 0][value_column].sum(), 2)
        amount_str = str(statons_amount)
        count_statons = df[df[value_column] != 0]['station'].unique()
        # count_statons_str = str(
        #     str(count_statons).replace('\n', '').replace(' ', ',').replace('[', '').replace(']', ''))

        text = f"""Till date{short_text_default} amounting to <b>₹ {amount_str}/-</b> \
        from <b>{len(count_statons)}</b> station(s)."""
        # - {count_statons_str}
    return text


if __name__ == '__main__':
    if config.ON_SERVER:
        os.system('pkill chrome -f')  # to kill the running chrome, if there's any
    load_dotenv()
    tz = pytz.timezone('Asia/Kolkata')  # to get the IST time zone (helpful for deployment)
    print('\n--------------------***--------------------\n')
    print(f'Execution Started at: {datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")}')
    print(f'Test:{TEST} | Sample:{SAMPLE} | Headless:{HEADLESS} |Send Email:{SEND_EMAIL} |\
    Send Fail Email:{SEND_FAIL_EMAIL}')
    t1 = time.time()

    today = datetime.datetime.now(tz).strftime("%Y-%m-%d")
    hour = datetime.datetime.now(tz).strftime("%H:%M")

    # Setting the time condition for evening and EOD report
    cut_off_hour = '22:00'
    con = hour >= cut_off_hour
    report_time = np.where(con, 'EOD', 'Evening')

    # File Path
    subj = f'{str(today)}_AmazonCashReport_{str(report_time)}'
    data_folderpath = toolkit.create_folder(projectname=PROJECT_NAME,
                                            foldername='data')  # create the parent folder and returns the path

    temp_folderpath = toolkit.create_folder(projectname=PROJECT_NAME,
                                            foldername='temp')  # create the temp folder and returns the path
    ssc_url = "https://www.amazonlogistics.eu/station/dashboard/"
    url_page = ssc_url + "problemsolvemanage"

    main_driver_recon_df = pd.DataFrame()
    main_bank_deposit_df = pd.DataFrame()
    main_running_bal_df = pd.DataFrame()
    failed_sites = pd.DataFrame()
    failed = []

    try:
        creds_all = site_login.create_site_login_creds_df(db=db_connector.connect_to_db('whizzard'))

    except Exception as e:
        print(f'Connection to DB Failed. Reason:{type(e).__name__}')
        print('Fetching Amazon Sites Data from Excel sheet.')
        creds_all = pd.DataFrame()

    xl_path = f'/home/ubuntu/atom/{PROJECT_NAME}/amazon_sites.xlsx' \
        if config.ON_SERVER else f'../{PROJECT_NAME}/amazon_sites.xlsx'
    if creds_all.empty:
        failure_message = 'Unable to fetch Amazon site details from DB! please check.'
        print(failure_message)
        failure_subj = f'Failure: {str(today)}_AmazonCashReport_{str(report_time)}'
        toolkit.send_failure_email(send=SEND_FAIL_EMAIL, from_email=os.getenv('EMAIL_ID'),
                                   pwd=os.getenv('EMAIL_PASSWORD'),
                                   receiver_email=['mrjiteshjadhao@gmail.com'],
                                   email_subject=failure_subj, email_message=failure_message)
        creds_all = pd.read_excel(xl_path)
    else:  # if active_sites_df is not empty, store the df as excel to get the updated data for future use
        creds_all.to_excel(xl_path, index=False)
        print('Amazon Sites DataFrame has been stored as an Excel file.')

    creds_all = creds_all.rename(columns={'siteName': 'Site Name', 'siteCode': 'Site Code',
                                          'clientSiteCode': 'Client Station Code',
                                          'client': 'client_portal',
                                          'clientName': 'Client',
                                          'omName': 'OM', 'rmName': 'RM',
                                          'userName': 'UserName',
                                          'password': 'Password',
                                          'timeStr': 'CMS Time',
                                          'active': 'Active'})

    login_creds_df = filter_df_equal_to_value(df=creds_all, column_name='Active', value=True)
    req_clients = ['Amazon Node', 'Amazon Prime Now Node']
    login_creds_df = login_creds_df[login_creds_df.Client.isin(req_clients)].reset_index(drop=True)

    print(f'Total Number of Sites found: {str(len(login_creds_df))}')

    if SAMPLE:
        login_creds_df = login_creds_df.head(2)
    print(f'Number of Sites for scraping: {str(len(login_creds_df))}')

    for i in range(len(login_creds_df)):
        login_site_code = login_creds_df['Site Code'][i]
        username = login_creds_df['UserName'][i]
        password = login_creds_df['Password'][i]
        browser = toolkit.get_driver(downloads_folder=None, headless=HEADLESS)  # initiate the webdriver for each site
        logged_in = toolkit.check_login_success(toolkit.login_to_amazon(driver=browser,
                                                                        url=url_page,
                                                                        username=username,
                                                                        password=password),
                                                expected_url=url_page)
        if logged_in:
            print(f'{i+1}/{len(login_creds_df)} - {login_site_code} - Login success!', end=' - ')

            try:
                # For Pending Driver Recon
                pending_recon_df = get_the_driver_recon(driver=browser, page=ssc_url, site_code=login_site_code)[0]
                running_bal_df = get_the_driver_recon(driver=browser, page=ssc_url, site_code=login_site_code)[1]

                main_driver_recon_df = pd.concat([main_driver_recon_df, pending_recon_df])
                main_driver_recon_df = main_driver_recon_df.reset_index(drop=True)

                main_running_bal_df = pd.concat([main_running_bal_df, running_bal_df])
                main_running_bal_df = main_running_bal_df.reset_index(drop=True)

                # For bank deposits
                bank_deposit_df = get_the_bank_deposit(driver=browser, page=ssc_url, site_code=login_site_code)
                bank_deposit_df = merger_df_site_details(df1=bank_deposit_df, df2=login_creds_df, join_type='left',
                                                         cca_time=False)
                bank_deposit_df['Date'] = today
                bank_deposit_df['Time'] = hour
                bank_deposit_df['Code'] = bank_deposit_df['Code'].replace('.0', '')
                bank_deposit_df['Code'] = bank_deposit_df['Code'].fillna('Missing')

                main_bank_deposit_df = pd.concat([main_bank_deposit_df, bank_deposit_df])
                main_bank_deposit_df = main_bank_deposit_df.reset_index(drop=True)

                print('Scraping successful!')
                browser.close()

            except Exception as e:
                error_name = type(e).__name__
                print(f'Scraping failed. Reason: {error_name}!')
                failed.append(login_site_code)
                browser.close()

        else:  # this block will be executed if not logged in
            print(f'{i+1}/{len(login_site_code)} - {login_site_code} - Login failed.')
            failed.append(login_site_code)
            browser.close()

    main_driver_recon_df = replace_currency_str(df=main_driver_recon_df)
    main_driver_recon_df = convert_to_float_dtype(df=main_driver_recon_df, df_type=True)

    main_running_bal_df['Running Balance'] = round(main_running_bal_df['Running Balance'].astype(float), 2)
    main_driver_recon_df = merge_recon_w_category(df=main_driver_recon_df, df1=main_running_bal_df)

    main_driver_recon_df = total_due_amount(df=main_driver_recon_df)

    driver_recon = filter_df_equal_to_value(df=main_driver_recon_df, column_name='Type', value='DSP')
    pending_recon = pivoted_df_single_value(df=driver_recon,
                                            index_columns=['Site Code', 'Client', 'OM', 'RM', 'Date', 'Time'],
                                            value_column=['Total Amount Due'],
                                            agg_type='sum')

    pending_recon = filter_df_doesnot_equal_to_value(df=pending_recon, column_name='Total Amount Due', value=0)

    main_bank_deposit_df = replace_currency_str(df=main_bank_deposit_df)
    main_bank_deposit_df = convert_to_float_dtype(df=main_bank_deposit_df, df_type=False)
    main_bank_deposit_df = format_date_column(df=main_bank_deposit_df, date_column_name='Creation date')
    undeposited = filter_df_equal_to_value(main_bank_deposit_df, 'Code', 'Missing')

    failed_sites['Site Code'] = failed

    if not failed_sites.empty:
        failed_sites['Site Code'] = failed
        failed_sites = merger_df_site_details(df1=failed_sites, df2=login_creds_df, join_type='left', cca_time=False)
        failed_sites['Date'] = today
        failed_sites['Time'] = hour
    else:
        failed_sites['Site Code'] = failed
        failed_sites['Client'] = ''
        failed_sites['Date'] = today
        failed_sites['Time'] = hour

    uncreated = remit_not_created_amazon(df=main_bank_deposit_df, df2=login_creds_df)
    uncreated = merger_df_site_details(df1=uncreated, df2=login_creds_df, join_type='left', cca_time=False)

    if not uncreated.empty:
        main_created_df = pd.merge(uncreated,
                                   main_bank_deposit_df[['Site Code', 'Expected Amount', 'Creation date_new']],
                                   on='Site Code', how='left')

        uncreated = pivoted_df_single_value(df=main_created_df,
                                            index_columns=['Site Code', 'Date', 'Time',
                                                           'Remittance Created', 'Client', 'OM', 'RM'],
                                            value_column=['Expected Amount'],
                                            agg_type='mean')
        uncreated['Expected Amount'] = round(uncreated['Expected Amount'].astype(float), 2)
    else:
        print('Remittance for all stations have been created')

    remit_not_sub_older = filter_df_doesnot_equal_to_value(df=undeposited, column_name='Creation date_new',
                                                           value=undeposited['Date'])
    remit_not_sub_current = filter_df_equal_to_value(df=undeposited, column_name='Creation date_new',
                                                     value=undeposited['Date'])

    remit_not_sub_older = create_remit_df(df=remit_not_sub_older, older_remit=True)
    remit_not_sub_current = create_remit_df(df=remit_not_sub_current, older_remit=False)

    summary = create_summary(df1=remit_not_sub_older, df2=remit_not_sub_current, df3=pending_recon, df4=uncreated)

    main_bank_deposit_df = main_bank_deposit_df.drop(columns=['Creation date_new'])
    undeposited = undeposited.drop(columns=['Creation date_new'])

    summary_amzn_edsp = filter_df_keywords(df=summary, column_name='Client', keyword='Amazon Node')
    summary_amzn_pnn = filter_df_keywords(df=summary, column_name='Client', keyword='Amazon Prime Now Node')

    s3_storage = s3c.connect_to_s3_storage(os.getenv('AWS_ACCESS_KEY_ID'),
                                           os.getenv('AWS_SECRET_ACCESS_KEY'))
    atom_bucket = s3_storage.Bucket('atom-s3')  # selecting a bucket from the s3 storage

    # fetch the last 3 files from s3 bucket
    latest_file = sorted(s3c.get_all_excels(connected_bucket=atom_bucket, folder_name='whiz-amz-dsp-drivers-cash'),
                         reverse=True)[0]
    dsp_drivers_cash_df = s3c.concat_excel_sheets_to_df(connected_bucket=atom_bucket, excel_files=[latest_file],
                                                        sheet_num=0)
    dsp_loss_df = s3c.concat_excel_sheets_to_df(connected_bucket=atom_bucket, excel_files=[latest_file], sheet_num=1)
    dsp_oor_df = s3c.concat_excel_sheets_to_df(connected_bucket=atom_bucket, excel_files=[latest_file], sheet_num=2)
    current_month_str = datetime.datetime.now(tz).strftime("%Y-%m")
    dsp_loss_df['event_datetime_new'] = dsp_loss_df['event_datetime_new'].astype(str)
    dsp_oor_df['latest_event_datetime'] = dsp_oor_df['latest_event_datetime'].astype(str)
    dsp_loss_df = (dsp_loss_df[dsp_loss_df['event_datetime_new'].str.contains(current_month_str)]).reset_index(drop=True)
    dsp_oor_df = (dsp_oor_df[dsp_oor_df['latest_event_datetime'].str.contains(current_month_str)]).reset_index(drop=True)

    output_path = file_saving_path(name=subj, path=data_folderpath, client_type=True)
    writer = pd.ExcelWriter(output_path, engine=None)
    failed_sites.to_excel(writer, sheet_name='Scraping Failed', index=False)
    summary.to_excel(writer, sheet_name='Summary - COD OM wise', index=False)
    pending_recon.to_excel(writer, sheet_name='Cash with DAs - Unreconciled', index=False)
    undeposited.to_excel(writer, sheet_name='Remittance not deposited', index=False)
    uncreated.to_excel(writer, sheet_name='Remittance not created', index=False)
    dsp_drivers_cash_df.to_excel(writer, sheet_name='DSP Drivers Cash', index=False)
    dsp_loss_df.to_excel(writer, sheet_name='Potential Loss', index=False)
    dsp_oor_df.to_excel(writer, sheet_name='OOR No RTS', index=False)
    main_driver_recon_df.to_excel(writer, sheet_name='raw_data_driver_recon', index=False)
    main_bank_deposit_df.to_excel(writer, sheet_name='raw_data_bank_deposits', index=False)
    writer.save()

    failled_site_message_edsp = failed_site_message(df=failed_sites, client_name='Amazon Node')
    failled_site_message_pnn = failed_site_message(df=failed_sites, client_name='Amazon Prime Now Node')
    pending_recon_message_edsp = pending_recon_message(df=pending_recon, client_name='Amazon Node')
    pending_recon_message_pnn = pending_recon_message(df=pending_recon, client_name='Amazon Prime Now Node')
    pending_recon_message_dsp = short_excess_message(df=dsp_drivers_cash_df, client_name='Amazon Ecommerce')
    remittance_message_edsp = remittance_message(df=summary_amzn_edsp)
    remittance_message_pnn = remittance_message(df=summary_amzn_pnn)
    uncreated_message_edsp = uncreated_message(df=uncreated, client_name='Amazon Node')
    uncreated_message_pnn = uncreated_message(df=uncreated, client_name='Amazon Prime Now Node')
    loss_message = loss_oor_message(df=dsp_loss_df, df_type='loss')
    oor_message = loss_oor_message(df=dsp_oor_df, df_type='oor')

    from_address = os.getenv('EMAIL_ID')

    if TEST:
        TO = ['mrjiteshjadhao@gmail.com']
        CC = ['mrjiteshjadhao@gmail.com']
    else:
        TO = ['liferacer300@gmail.com']
        CC = ['jitesharvindjadhao1999@gmail.com']

    if hour >= cut_off_hour:
        message = [f"""Hi Team,\n\nPlease find attached cash management report for \
        <i><b>Amazon EDSP, Prime Now Node and DSP Stations</i></b>,\
        this is for your information and action.\
        Here is the summary of today's report:\n\
        <ul><b>For Amazon Node:</b>
        <ol><li>{failled_site_message_edsp}</li>\
        <li>{pending_recon_message_edsp}</li>\
        <li>{remittance_message_edsp}</li>\
        <li>{uncreated_message_edsp}</li></ol></ul>\
        <ul><b>For Amazon Prime Now Node Stations:</b>\
        <ol><li>{failled_site_message_pnn}</li>\
        <li>{pending_recon_message_pnn}</li>\
        <li>{remittance_message_pnn}</li>\
        <li>{uncreated_message_pnn}</li></ol></ul>\
        <ul><b>For Amazon DSP Stations:</b>\
        <ol><li>{pending_recon_message_dsp}</li></ol></ul>\
        <ul><b>Potential Loss for Amazon Stations: </b>\
        <ol><li>{loss_message}</li></ol></ul>\
        <ul><b>OOR No RTS for Amazon Stations: </b>\
        <ol><li>{oor_message}</li></ol></ul>\n\
        Regards\nAnalytics Team"""]

    else:
        CC.remove('mrjiteshjadhao@gmail.com') if 'mrjiteshjadhao@gmail.com' in CC else None
        message = [f"""Hi Team,\n\nPlease find attached cash management report for \
        <i><b>Amazon EDSP, Prime Now Node and DSP Stations</i></b>,\
        this is for your information and action.\
        Here is the summary of today's report:\n\
        <ul><b>For Amazon Node:</b>
        <ol><li>{failled_site_message_edsp}</li>\
        <li>{remittance_message_edsp}</li>\
        <ul><b>For Amazon Prime Now Node Stations:</b>\
        <ol><li>{failled_site_message_pnn}</li>\
        <li>{remittance_message_pnn}</li></ol></ul>\
         <ul><b>For Amazon DSP Stations:</b>\
        <ol><li>{pending_recon_message_dsp}</li></ol></ul>\
        <ul><b>Potential Loss for Amazon Stations: </b>\
        <ol><li>{loss_message}</li></ol></ul>\
        <ul><b>OOR No RTS for Amazon Stations: </b>\
        <ol><li>{oor_message}</li></ol></ul>\n\
        Regards\nAnalytics Team"""]

    toolkit.send_email(send=SEND_EMAIL, from_email=from_address,
                       pwd=os.getenv('EMAIL_PASSWORD'),
                       receiver_email=TO, copy=CC, email_subject=subj,
                       email_message=message, attachment_file=[output_path])

    if not SAMPLE:  # upload final output into s3 bucket, only if it's not sample
        s3c.upload_to_s3(connected_bucket=atom_bucket,
                         folder_name='amazon-edsp-cash-management-check',
                         filepath=output_path,
                         destination_filename=subj + '.xlsx')

    if config.ON_SERVER:
        os.system('pkill chrome -f')  # to kill the running chrome, if there's any

    print(f'Total Time for the complete execution: {(time.time() - t1) / 60:.2f} minutes')
    print(f'Execution Completed at: {datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")}')
