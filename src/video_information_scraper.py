import os.path
from time import sleep
import datetime
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
'''
proxy
'''
import socks, socket


class ChannelCountryAndScraper(object):

    def __init__(self):
        self.video_urls = []
        self.nihongo_channel_countries = []
        self.channel_subscribers_true = []
        self.search_data_csv_file_name = "./../data/search/youtube_search_csv_data"
        self.search_data_csv_file_path = os.path.join(os.getcwd(), self.search_data_csv_file_name+'.csv')
        self.scarch_videos_list_csv_file_name = "./../data/search/scarch_videos_list_scv"
        self.scarch_videos_list_csv_file_path = os.path.join(os.getcwd(), self.scarch_videos_list_csv_file_name+'.csv')


    def run(self):
        self.copy_csv()
        self.csv_file_drop_duplicate()
        self.read_csv_urls()
        self.get_page_source()
        # self.add_as_csv_file()


    def copy_csv(self):
        df = pd.read_csv(self.search_data_csv_file_path)
        pd.DataFrame(df).to_csv(self.scarch_videos_list_csv_file_path,index=False)
        print(self.scarch_videos_list_csv_file_path+"にコピーしました")


    def csv_file_drop_duplicate(self):
        df = pd.read_csv(self.scarch_videos_list_csv_file_path, engine='python')
        df_drop_duplicate = df.drop_duplicates(subset='video_url', keep='last')
        df_add_csv = pd.DataFrame(df_drop_duplicate).to_csv(self.scarch_videos_list_csv_file_path,index=False)
        print(self.scarch_videos_list_csv_file_path+"重複動画削除")


    def read_csv_urls(self):
        self.df = pd.read_csv(self.scarch_videos_list_csv_file_path, engine='python')
        channel_url_data = self.df.set_index('video_url')
        video_url_data = channel_url_data.index.values
        video_urls = video_url_data.tolist()
        for i in video_urls:
            youtube_url = 'https://www.youtube.com'
            self.video_url = ('%s' % i)
            video_url_path = urlparse.urljoin(youtube_url, self.video_url)
            self.video_urls.append(video_url_path)


    def get_page_source(self):
        for i in self.video_urls:
            print(i)
            html = requests.get('http://localhost:8050/render.html',
            params={'url': i, 'wait': 2})
            self.video_url_i = i
            self.soup = BeautifulSoup(html.text, "html.parser")
            self.parse_view_and_createAt()
            self.parse_video_tags()
            self.parse_video_description()
            self.parse_video_like()
            self.only_last_tags()
            self.scarch_videos_list_csv_scarch_column()
            self.add_as_csv_file()
            self.scarch_videos_list_csv_duplicate()


    def parse_view_and_createAt(self):
        self.views = []
        self.create_ats = []
        self.tag_str = []
        for i in self.soup.find_all('div', {"class" : "style-scope ytd-video-primary-info-renderer"}):
            view_i = re.findall('</ytd-badge-supported-renderer><div class="style-scope ytd-video-primary-info-renderer" id="info"><div class="style-scope ytd-video-primary-info-renderer" id="info-text"><div class="style-scope ytd-video-primary-info-renderer" id="count"><yt-view-count-renderer class="style-scope ytd-video-primary-info-renderer" small_=""><!--css-build:shady--><span class="view-count style-scope yt-view-count-renderer">.*</span><span class="short-view-count style-scope yt-view-count-renderer">', str(i))
            view_i_join = ",".join(view_i)
            view_i_join_replace = str(view_i_join).replace('</ytd-badge-supported-renderer><div class="style-scope ytd-video-primary-info-renderer" id="info"><div class="style-scope ytd-video-primary-info-renderer" id="info-text"><div class="style-scope ytd-video-primary-info-renderer" id="count"><yt-view-count-renderer class="style-scope ytd-video-primary-info-renderer" small_=""><!--css-build:shady--><span class="view-count style-scope yt-view-count-renderer">', '').replace(' views</span><span class="short-view-count style-scope yt-view-count-renderer">', '')
            view = view_i_join_replace.replace(',', '')
            create_at_i = re.findall('views</span></yt-view-count-renderer></div><div class="style-scope ytd-video-primary-info-renderer" id="date"><span class="style-scope ytd-video-primary-info-renderer" id="dot">•</span><yt-formatted-string class="style-scope ytd-video-primary-info-renderer">.*</yt-formatted-string>', str(i))
            create_at_i_join = ",".join(create_at_i)
            create_at_join_replace = create_at_i_join.replace('views</span></yt-view-count-renderer></div><div class="style-scope ytd-video-primary-info-renderer" id="date"><span class="style-scope ytd-video-primary-info-renderer" id="dot">•</span><yt-formatted-string class="style-scope ytd-video-primary-info-renderer">', '').replace('</yt-formatted-string>', '')
            create_at_l = create_at_join_replace.replace(',', '')
            if view is None:
                continue
            if create_at_l is None:
                continue
            if not view == '':
                self.views.append(view)
                create_at = str(create_at_l).replace("['", '').replace("']", '')
                try:
                    create_at = datetime.datetime.strptime(create_at, '%b %d %Y').strftime('%Y/%m/%d')
                except ValueError:
                    create_at = None
                self.create_ats.append(create_at)
                print(self.views)
                print(self.create_ats)


    def parse_video_tags(self):
        self.tags = []
        for i in self.soup.find_all('meta'):
            tag_i = re.findall('<meta content.*property="og:video:tag"/>', str(i))
            tag_i_join = ",".join(tag_i)
            tag_l = tag_i_join.replace('<meta content="', '').replace('" property="og:video:tag"/>', '')
            if tag_l is None:
                continue
            if not tag_l == '':
                tag = (tag_l).replace("['", '').replace("']", '')
                self.tags.append(tag)


    def only_last_tags(self):
        tags_i = self.tags
        if tags_i is None:
            tags = None
            self.tag_str.append(tags)
        else:
            tags = ','.join(tags_i)
            self.tag_str.append(tags)
            print(self.tag_str)


    def parse_video_description(self):
        self.descriptions = []
        for i in self.soup.find_all('meta'):
            description_i = re.findall('<meta content.*property="og:description"/>', str(i))
            description_i_join = ",".join(description_i)
            description_l = description_i_join.replace('<meta content="', '').replace('" property="og:description"/>', '')
            if description_l is None:
                continue
            if not description_l == '':
                description = (description_l).replace("['", '').replace("']", '')
                self.descriptions.append(description)
                print(self.descriptions)


    def parse_video_like(self):
        self.likes = []
        self.dislikes = []
        for i in self.soup.find_all('yt-formatted-string'):
            like_i = re.findall('aria-label.* likes', str(i))
            like_i_join = ",".join(like_i)
            like_l = like_i_join.replace('aria-label="', '').replace(' likes', '').replace(',', '')
            dislike_i = re.findall('aria-label.*dislikes', str(i))
            dislike_i_join = ",".join(dislike_i)
            dislike_l = dislike_i_join.replace('aria-label="', '').replace(' dislikes', '').replace(',', '')
            if like_l is None:
                continue
            if dislike_l is None:
                continue
            if not like_l == '':
                like = str(like_l).replace("['", '').replace("']", '')
                self.likes.append(like)
                print(self.likes)
            if not dislike_l == '':
                like = str(like_l).replace("['", '').replace("']", '')
                self.dislikes.append(dislike_l)
                print(self.dislikes)


    # def add_as_csv_file(self):
    #     df = pd.read_csv(self.scarch_videos_list_csv_file_path)
    #     df['view'] = self.views
    #     df['create_at'] = self.create_ats
    #     df['tag'] = self.tag_str
    #     df['description'] = self.descriptions
    #     df['like'] = self.likes
    #     df['dislike'] = self.dislikes
    #     df_add = pd.DataFrame(df).to_csv(self.scarch_videos_list_csv_file_path, mode='a', header=False,index=False)


    def scarch_videos_list_csv_scarch_column(self):
        video_url_i = self.video_url_i
        video_url = video_url_i.replace('https://www.youtube.com', '')
        print(video_url)
        mask = self.df['video_url'] == video_url
        self.true_column = self.df[mask]


    def add_as_csv_file(self):
        try:
            self.true_column['view'] = self.views
        except AttributeError:
            print(self.views)
            self.true_column['view'] = "エラー"
        except ValueError:
            print(self.views)
            self.true_column['view'] = "エラー"
        try:
            self.true_column['create_at'] = self.create_ats
        except AttributeError:
            print(self.create_ats)
            self.true_column['create_at'] = "エラー"
        except ValueError:
            print(self.channel_length)
            self.true_column['create_at'] = "エラー"
        try:
            self.true_column['tag'] = self.tag_str
        except AttributeError:
            print(self.tag_str)
            self.true_column['tag'] = "エラー"
        except ValueError:
            print(self.tag_str)
            self.true_column['tag'] = "エラー"
        try:
            self.true_column['description'] = self.descriptions
        except AttributeError:
            print("エラー")
            print(self.descriptions)
            self.true_column['description'] = "エラー"
        except ValueError:
            print(self.descriptions)
            self.true_column['description'] = "エラー"
        try:
            self.true_column['like'] = self.likes
        except ValueError:
            self.likes = "非表示"
            self.true_column['like'] = self.likes
        try:
            self.true_column['dislike'] = self.dislikes
        except ValueError:
            self.dislikes = "非表示"
            self.true_column['dislike'] = self.dislikes
        pd.DataFrame(self.true_column).to_csv(self.scarch_videos_list_csv_file_path, mode='a', header=False, index=False)
        print(self.scarch_videos_list_csv_file_path+"に追記しました")


    def scarch_videos_list_csv_duplicate(self):
        df = pd.read_csv(self.scarch_videos_list_csv_file_path, engine='python')
        df_drop_duplicate = df.drop_duplicates(subset='channel_url', keep='last')
        pd.DataFrame(df_drop_duplicate).to_csv(self.scarch_videos_list_csv_file_path,index=False)
        print(self.scarch_videos_list_csv_file_path+"の重複削除しました")


if __name__ == "__main__":
    channel_country = ChannelCountryAndScraper()
    channel_country.run()
