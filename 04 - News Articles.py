# Databricks notebook source
def icc_news_scrape():
    icc_news = "https://www.icc-cricket.com/news"
    icc_news_request = requests.get(icc_news)
    soup = BeautifulSoup(icc_news_request.text, "html.parser")
    articles =  soup.find('section', class_='widget news-index').find_all("a")
    articles_contents = {}
    for article in articles:
        article_contents = ""
        article_request = requests.get("https://www.icc-cricket.com/"+article["href"])
        article_soup = BeautifulSoup(article_request.text, "html.parser")
        summary = article_soup.find('p', class_="article__summary").text
        article_contents += summary
        article_contents += "\n"
        article_content = article_soup.find('div', class_="article__content")
        paragraphs = article_content.find_all('p')
        for paragraph in paragraphs:
            article_contents += paragraph.text
            article_contents += "\n"
        articles_contents[article["href"]] = article_contents
    return articles_contents

import os 

#give the full path for the directory input 
def save_icc_news_files(directory, articles_dict):
    dir_contents = os.listdir(directory)
    for article_name in articles_dict.keys():
        file_name = str(article_name)+".txt"
        if file_name not in dir_contents:
            file_name = directory+file_name
            with open(file_name, "w") as file:
                file.write(str(articles_dict[article_name]))
