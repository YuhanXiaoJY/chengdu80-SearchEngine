#!/usr/bin/env python
import json
import re
from ElasticSearchHttpHelper import ElasticSearchHttpHelper


class ExtractUniversity:
    output_file = "data/university.CSV"
    university_influence_dict = {}
    researcher_url_dict = {}
    researcher_influence_dict = {}

    def get_url(self, filename):
        file = open(filename, 'r', encoding='utf-8')
        line = file.readline().strip()
        cnt = 0
        while line:
            cnt += 1
            print(cnt)
            json_dict = json.loads(line)
            name = json_dict['name']
            url = json_dict['webpage_url']
            self.researcher_url_dict[name] = url
            line = file.readline()
        file.close()

    def get_researcher_url_dict(self):
        filename1 = "data/out_out_faculty_members_part1_with_homepage.json"
        filename2 = "data/out_out_faculty_members_part2_with_homepage.json"
        self.get_url(filename1)
        self.get_url(filename2)


    def get_University(self, url):
        res_list = re.findall(r"\.(.+?)\.edu/", url)
        if len(res_list) > 0:
            list = res_list[0].split('.')
            print(list)
            return list[len(list) - 1]
            # return res_list[0]
        else:
            return None


    def get_researcher_influence_dict(self):
        helper = ElasticSearchHttpHelper()
        helper.get_researcher_influence_dict()
        self.researcher_influence_dict = helper.researcher_influence_dict

    def extract_university(self):
        self.get_researcher_url_dict()
        self.get_researcher_influence_dict()

        for name in self.researcher_influence_dict.keys():
            if name in self.researcher_url_dict.keys():
                url = self.researcher_url_dict[name]
                paper_cnt = self.researcher_influence_dict[name][0]
                citation_cnt = self.researcher_influence_dict[name][1]
                univ = self.get_University(url)
                if univ == None:
                    continue
                if univ in self.university_influence_dict.keys():
                    self.university_influence_dict[univ][0] += int(paper_cnt)
                    self.university_influence_dict[univ][1] += int(citation_cnt)
                else:
                    self.university_influence_dict[univ] = [int(paper_cnt), int(citation_cnt)]

        file = open(self.output_file, 'w', encoding='utf-8')
        for univ in self.university_influence_dict.keys():
            file.write(univ + ',' + str(self.university_influence_dict[univ][0]) + ',' + str(self.university_influence_dict[univ][1]) + '\n')
        file.close()


if __name__ == "__main__":
    extract_manager = ExtractUniversity()
    extract_manager.extract_university()