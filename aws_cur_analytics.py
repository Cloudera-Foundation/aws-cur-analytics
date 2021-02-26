#
#
#
# Run analytics and output visualizations for AWS CUR data from grantees
#
#
#

import pandas as pd
from tabulate import tabulate
import numpy as np
import matplotlib.pyplot as plt
import glob
import boto3
import botocore
import os
import re
from configparser import ConfigParser
import logging
import json
import seaborn as sns
from datetime import datetime


# Define configuration file
config = ConfigParser()
gconfig = ConfigParser()
config.read('config.ini')
gconfig.read(config.get('main','grantees_file'))
loglevel=config.get('main','log_level')

# Setup logging level
logging.basicConfig(level=loglevel)
logger = logging.getLogger()

# Read in the config.ini sections, which would correlate to grantee shortnames, and initialize some vars
grantees=gconfig.sections()
dframesorg = {}
local_gfiles = {}
datedirs = {}
local_csv_dir = config.get('main','local_data_dir')
ngrantees = len(grantees)
ind = np.arange(ngrantees)

# Create local dirs to store AWS CUR CSV files per grantee
for g in grantees:
    if not os.path.exists(local_csv_dir + "/" + g):
        logger.info(f" Create local data dir for grantee {g}")
        os.makedirs(local_csv_dir + "/" + g)

# Download latest source AWS CUR JSON and CSV files for each grantee
def download_AWS_CUR():
    for g in grantees:
        datedirs[g] = []
        s3_resource = boto3.resource('s3')
        gbucket = s3_resource.Bucket(gconfig.get(g,'s3_bucket'))
        objects = gbucket.objects.filter(Prefix=gconfig.get(g,'s3_prefix'))
        s3_prefix = gconfig.get(g,'s3_prefix')
        s3_cur_rpt_name = gconfig.get(g,'s3_cur_rpt_name')
        try:
            for obj in objects:
                # obj.key CUR example: <bucket>/<prefix>/<CUR_report_name>/20210201-20210301/1efe213b-eed6-4924-bad0-6e74f21d60e5/CFAWSCostReportRegular-1.csv.gz
                # obj.key CUR manifest example: <bucket>/<prefix>/<CUR_report_name>/20210201-20210301/1efe213b-eed6-4924-bad0-6e74f21d60e5/CFAWSCostReportRegular-Manifest.json
                # obj.key CUR overwrite example: <bucket>/<prefix>/<CUR_report_name>/20210201-20210301/CFAWSCostReportRegularOverwrite-00001.csv.gz
                # obj.key CUR overwrite manifest example: <bucket>/<prefix>/<CUR_report_name>/20210201-20210301/CFAWSCostReportRegularOverwrite-Manifest.json
                # Sample format: s3://<bucket>/<prefix>/CFAWSCostReportRegular/20210201-20210301/1efe213b-eed6-4924-bad0-6e74f21d60e5/CFAWSCostReportRegular-1.csv.gz to data_csv//cf/1efe213b-eed6-4924-bad0-6e74f21d60e5.csv.gz

                # Filter out just the JSON files (there are other files in S3 that are listed, such as csv files, but we will pull those after we get the path from JSON file)
                if re.search('.json', obj.key):
                    json_file_split = obj.key.split("/")
                    if json_file_split[2] not in datedirs[g]:
                        datedirs[g].append(json_file_split[2])

            for datedir in datedirs[g]:
                local_date_dir = local_csv_dir + "/" + g + "/" + datedir
                if not os.path.exists(local_date_dir):
                    logger.info(f" Create local date dir, {local_date_dir}, for grantee {g}")
                    os.makedirs(local_date_dir)
                json_local_file = local_date_dir + "/" + s3_cur_rpt_name + "-Manifest.json"
                json_s3_file = s3_prefix + "/" + s3_cur_rpt_name + "/" + datedir + "/" + s3_cur_rpt_name + "-Manifest.json"
                # The reason we have to download the same file for every month is because that file changes daily for the current month (points to new CSV file).
                # Improvement: Dont download older month JSON files
                logger.info(f"Downloading JSON file {json_s3_file} to {json_local_file}")
                gbucket.download_file(json_s3_file, json_local_file)
                with open(json_local_file) as f:
                    data = json.load(f)
                    csvfile = data['reportKeys']
                    # Improvement: Handle potentially multiiple CSV files (ie: CFAWSCostReportRegular-1.csv.gz, CFAWSCostReportRegular-2.csv.gz, etc), which is possible if csv file is too large
                    csv_local_file = local_date_dir + "/" + s3_cur_rpt_name + "-1.csv.gz"
                    logger.info(f"Downloading CSV file {csvfile[0]} to {csv_local_file}")
                    gbucket.download_file(csvfile[0], csv_local_file)

        except botocore.errorfactory.ClientError as error:
            logger.error(f" No bucket found for {g}")

# Read in the gzipped AWS CUR source csv files and write to grantee-specific dataframes
# Here we are creating a dictionary (dframesorg) of dataframes (dframesorg[g]), where the dictionary key is the grantee name (g) and the dictionary value is the dataframe itself
# We do this in order to create grantee-specific dataframes (variablize it). Another option to consider is to create a multi-endex dataframe
#            print(type(dframesorg))
#               <class 'dict'>
#            print(type(dframesorg[g]))
#               <class 'pandas.core.frame.DataFrame'>
#            print(dframesorg.keys())
#               dict_keys(['cf', 'pon', 'mej', 'wwb'])
def create_DF():
    for g in grantees:
        try:
            single_df = [pd.read_csv(gfile, compression='gzip') for gfile in glob.glob(f"{local_csv_dir}/{g}/*/*.gz")]
            dframesorg[g] = pd.concat(single_df, axis=0)
        except ValueError:
            logger.error(f" No files found in {local_csv_dir}/{g}/")

# Aggregate for total cost per grantee
def agg_TotalCost_Grantee():
    total_costs={}
    gsum={}
    bwidth = .40
    fig, ax = plt.subplots()
    for g in grantees:
        try:
            total_costs[g] = dframesorg[g][['lineItem/BlendedCost']]
            total_costs[g] = total_costs[g].rename(columns= {'lineItem/BlendedCost':'BlendedCost'})
            gsum[g] = total_costs[g].sum()
            logger.debug(f" Total Costs for Each Grantee {g}")
            logger.debug(gsum[g])
            plt.bar(g, gsum[g], bwidth, color=gconfig.get(g,'chart_color'),label=g)
        except KeyError:
            logger.error(f" No dataframe defined for {g}")
    plt.ylabel('Dollars (USD)')
    plt.xlabel('Grantee')
    plt.title('Total AWS Costs per Grantee')
    plt.legend()
    plt.draw()

def agg_TotalCost_Grantee_Pie():
    total_costs={}
    gsum={}
    fig, axes = plt.subplots(1, ngrantees)
    x=0
    i=0
    for g in grantees:
        try:
            total_costs[g] = dframesorg[g][['lineItem/BlendedCost']]
            total_costs[g] = total_costs[g].rename(columns= {'lineItem/BlendedCost':'BlendedCost'})
            gsum[g] = total_costs[g].sum()
            logger.debug(f" Total Costs for Each Grantee {g}")
            logger.debug(gsum[g])
            usd_leftover = int(gconfig.get(g,'aws_grant_amt')) - gsum[g]['BlendedCost']
            # Pie chart can't take any negative values
            if usd_leftover < 0:
                logger.error(f"ALERT: WE PASSED THE AWS GRANT AMOUNT for Grantee {g}")
            if gsum[g]['BlendedCost'] < 0:
                logger.error(f"AWS total cost for grantee {g} is negative, {gsum[g]['BlendedCost']}")
                gsum[g]['BlendedCost'] = 0
            costs = [usd_leftover, gsum[g]['BlendedCost']]
            fig.axes[i].set_title("{}".format(g))
            fig.axes[i].pie(costs, labels=['AWS Remaining', 'Spent'], labeldistance=0.7, colors=['#3CB371', gconfig.get(g,'chart_color')], center=(x,0), explode=(0,.2), shadow=True, startangle=-45)
            x += 2
            i += 1
        except KeyError:
            logger.error(f" No dataframe defined for {g}")
    fig.suptitle('Total AWS Costs per Grantee')
    plt.subplots_adjust(top=0.55)

def daily_Cost_Per_Grantee():
    daily_costs={}
    bwidth = .4
    fig, ax = plt.subplots()
    # Aggregate for daily cost per grantee
    for g in grantees:
        try:
            daily_costs[g] = dframesorg[g][['lineItem/UsageEndDate','lineItem/BlendedCost']]
            daily_costs[g] = daily_costs[g].rename(columns= {'lineItem/UsageEndDate':'EndDate','lineItem/BlendedCost':'BlendedCost'})
            daily_costs[g] = daily_costs[g].replace({'EndDate':r'T[0-9][0-9]:[0-9][0-9]:[0-9][0-9]Z'}, value='', regex=True)
            daily_costs[g]['EndDate'] = pd.to_datetime(daily_costs[g]['EndDate'], format='%Y-%m-%d')
            daily_costs[g] = daily_costs[g].groupby('EndDate').sum()
            daily_costs[g] = daily_costs[g].reset_index()
            ax.bar(daily_costs[g]['EndDate'], daily_costs[g]['BlendedCost'], bwidth, color=gconfig.get(g,'chart_color'), label=g)
        except KeyError:
            logger.error(f" No dataframe defined for {g}")
    ax.set_ylabel('Dollars (USD)')
    ax.set_xlabel('Date')
    ax.set_title(f"AWS Daily Cost for Grantees")
    ax.legend()
    plt.xticks(rotation=-45)


def monthly_Cost_Per_Grantee():
    monthly_costs={}
    bwidth = .4
    fig, ax = plt.subplots(ncols=ngrantees, sharex=True, sharey=True)
    i=0
    # Aggregate for monthly cost per grantee
    for g in grantees:
        try:
            # Use UsageStartDate instead of UsageEndDate since the EndDate can be the 1st of the next month in many cases, which throws off the charts
            monthly_costs[g] = dframesorg[g][['lineItem/UsageStartDate','lineItem/BlendedCost']]
            monthly_costs[g] = monthly_costs[g].rename(columns= {'lineItem/UsageStartDate':'StartDate','lineItem/BlendedCost':'BlendedCost'})
            monthly_costs[g]['StartDate'] = pd.to_datetime(monthly_costs[g]['StartDate'], format="%Y-%m-%dT%H:%M:%S")
            #print(monthly_costs[g].dtypes)
            # index                        int64
            # StartDate        datetime64[ns, UTC]
            # BlendedCost                float64
            # dtype: object
            monthly_costs[g]['StartDate'] = monthly_costs[g]['StartDate'].dt.strftime('%Y-%m')
            #print(monthly_costs[g].dtypes)
            # index            int64
            # StartDate         object
            # BlendedCost    float64
            # dtype: object
            monthly_costs[g] = monthly_costs[g].groupby('StartDate').sum()
            ## Troubleshooting
            #print(tabulate(monthly_costs[g], headers='keys', tablefmt='psql', showindex="always"))
            #print(monthly_costs[g].index)
            #print(monthly_costs[g].columns)
            #print(monthly_costs[g].dtypes)
            # index            int64
            # BlendedCost    float64
            # dtype: object
            #logger.info(f"Monthly Costs per Grantee {g}")
            #logger.info(monthly_costs[g])
            monthly_costs[g] = monthly_costs[g].reset_index()
            #gplot=sns.barplot(x=monthly_costs[g].index, y=monthly_costs[g].BlendedCost, data=monthly_costs[g], ax=ax[i], color=gconfig.get(g,'chart_color'), hue=monthly_costs[g].index, dodge=False)
            gplot=sns.barplot(x='StartDate', y='BlendedCost', data=monthly_costs[g], ax=ax[i], color=gconfig.get(g,'chart_color'), label=g)
            gplot.set_xticklabels(gplot.get_xticklabels(), rotation=-45)
            plt.setp(ax[i],xlabel='Date')
            plt.setp(ax[i],ylabel='Dollars (USD)')
            i += 1
        except KeyError:
            logger.error(f" No dataframe defined for {g}")
    fig.suptitle(f"AWS Monthly Cost for Grantees")
    fig.legend()


download_AWS_CUR()
create_DF()
agg_TotalCost_Grantee()
agg_TotalCost_Grantee_Pie()
daily_Cost_Per_Grantee()
monthly_Cost_Per_Grantee()
plt.show()