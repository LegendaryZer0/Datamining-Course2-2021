import scrapy


class FirstSpyder(scrapy.Spider):
    code = 'huina'
    name = "simplespider"
    start_urls = [


        "https://www.gosuslugi.ru/",
        "https://www.gosuslugi.ru/feedback",
    ]

    def parse(self, response):
        page = response.url.split('/')[-2]
        filename= 'gosUsl-%s.html' % page
        with open(filename,"wb") as f:
            f.write(response.body)