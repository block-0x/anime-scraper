import os.path
from time import sleep
import datetime
import datetime as dt
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.common.keys import Keys
import re
import numpy as np
import regex
import json
import requests
try:
    import urlparse
except ImportError:
    import urllib.parse as urlparse
import urllib.request, urllib.error


class GetChannelInformationOfVideo(object):

    def __init__(self):
        self.scarch_videos_list_channel_urls = []
        self.channel_list_update_channel_urls = []
        self.channel_list_update_csv_file_name = "./../data/channel/youtube_channel_list_update"
        self.channel_list_update_csv_file_path = os.path.join(os.getcwd(), self.channel_list_update_csv_file_name+'.csv')
        self.search_data_csv_file_name = "./../data/search/youtube_search_csv_data"
        self.search_data_csv_file_path = os.path.join(os.getcwd(), self.search_data_csv_file_name+'.csv')
        self.scarch_videos_list_csv_file_name = "./../data/search/scarch_videos_list_scv"
        self.scarch_videos_list_csv_file_path = os.path.join(os.getcwd(), self.scarch_videos_list_csv_file_name+'.csv')


    def run(self):
    	self.copy_csv()
    	self.scarch_videos_list_csv_drop_duplicate()
    	self.read_scarch_videos_list_channel_csv()
    	self.read_channel_list_update_csv_channel_urls()
    	self.get_channel_infomation()
    	self.scarch_videos_list_csv_duplicate()


    def copy_csv(self):
        df = pd.read_csv(self.search_data_csv_file_path)
        df_scrape_at_this_month = df[df['scrape_at'] > dt.datetime(2020,8,10).strftime("%Y/%m/%d")]
        if 0 is os.path.getsize(self.scarch_videos_list_csv_file_path):
        	pd.DataFrame(df_scrape_at_this_month).to_csv(self.scarch_videos_list_csv_file_path, index=False, header=True)
        	print(self.scarch_videos_list_csv_file_path+"にコピーしました")
        else:
	        pd.DataFrame(df_scrape_at_this_month).to_csv(self.scarch_videos_list_csv_file_path, mode='a', header=False, index=False)
	        print(self.scarch_videos_list_csv_file_path+"に追加コピーしました")


    def scarch_videos_list_csv_drop_duplicate(self):
        scarch_videos_list_csv_df = pd.read_csv(self.scarch_videos_list_csv_file_path, engine='python')
        scarch_videos_list_csv_df_drop_duplicate = scarch_videos_list_csv_df.drop_duplicates(subset='video_url', keep='first')
        pd.DataFrame(scarch_videos_list_csv_df_drop_duplicate).to_csv(self.scarch_videos_list_csv_file_path, index=False)
        print(self.scarch_videos_list_csv_file_path+"重複videoを削除しました")


    def read_scarch_videos_list_channel_csv(self):
        self.scarch_videos_list_channel_url_data = pd.read_csv(self.scarch_videos_list_csv_file_path, engine='python')
        scarch_videos_list_channel_urls_ndarray = self.scarch_videos_list_channel_url_data.index.values
        scarch_videos_list_channel_urls = scarch_videos_list_channel_urls_ndarray.tolist()
        for i in scarch_videos_list_channel_urls:
            self.scarch_videos_list_channel_url = ('%s' % i)
            self.scarch_videos_list_channel_urls.append(self.scarch_videos_list_channel_url)


    def read_channel_list_update_csv_channel_urls(self):
        self.channel_list_update_channel_url_data = pd.read_csv(self.channel_list_update_csv_file_path, engine='python')


    def get_channel_infomation(self):
        for i in self.scarch_videos_list_channel_urls:
            print(i)
            self.scarch_videos_list_channel_url = i
            self.channel_list_update_csv_scarch_column()
            self.extraction_of_channel_list_update_csv()
            self.search_video_list_csv_scarch_column()
            self.write_of_search_video_list_csv()


    def channel_list_update_csv_scarch_column(self):
        channel_list_update_mask = self.channel_list_update_channel_url_data['channel_url'] == self.scarch_videos_list_channel_url
        self.channel_list_channel_list_true_column = self.channel_list_update_channel_url_data[channel_list_update_mask]
        print(self.channel_list_channel_list_true_column)


    def extraction_of_channel_list_update_csv(self):
    	self.channel_country = self.channel_list_channel_list_true_column['channel_country']
    	self.channel_subscriber = self.channel_list_channel_list_true_column['channel_subscriber']
    	self.mean_view = self.channel_list_channel_list_true_column['mean_view']


    def search_video_list_csv_scarch_column(self):
        search_video_list_mask = self.scarch_videos_list_channel_url_data['channel_url'] == self.scarch_videos_list_channel_url
        self.search_video_list_true_column = self.scarch_videos_list_channel_url_data[search_video_list_mask]


    def write_of_search_video_list_csv(self):
    	try:
            self.search_video_list_true_column['channel_country'] = self.channel_country
            print(self.channel_country)
    	except AttributeError:
            self.search_video_list_true_column['channel_country'] = "エラー"
            print(self.channel_country)
    	except ValueError:
            self.search_video_list_true_column['channel_country'] = "エラー"
            print(self.channel_country)
    	try:
            self.search_video_list_true_column['channel_subscriber'] = self.channel_subscriber
            print(self.channel_subscriber)
    	except AttributeError:
            self.search_video_list_true_column['channel_subscriber'] = "エラー"
            print(self.channel_subscriber)
    	except ValueError:
            self.search_video_list_true_column['channel_subscriber'] = "エラー"
            print(self.channel_subscriber)
    	try:
            self.search_video_list_true_column['mean_view'] = self.mean_view
            print(self.mean_view)
    	except AttributeError:
            self.search_video_list_true_column['mean_view'] = "エラー"
            print(self.mean_view)
    	except ValueError:
            self.search_video_list_true_column['mean_view'] = "エラー"
            print(self.mean_view)
    	pd.DataFrame(self.search_video_list_true_column).to_csv(self.scarch_videos_list_csv_file_path, mode='a', header=False, index=False)
    	print(self.scarch_videos_list_csv_file_path+"に追記しました")


    def scarch_videos_list_csv_duplicate(self):
        df = pd.read_csv(self.scarch_videos_list_csv_file_path, engine='python')
        df_drop_duplicate = df.drop_duplicates(subset='channel_url', keep='last')
        pd.DataFrame(df_drop_duplicate).to_csv(self.scarch_videos_list_csv_file_path, index=False)
        print(self.scarch_videos_list_csv_file_path+"の重複削除しました")


if __name__ == "__main__":
    get_info = GetChannelInformationOfVideo()
    get_info.run()
