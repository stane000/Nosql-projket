# Requires the PyMongo package.
# https://api.mongodb.com/python/current

import time
from typing import Dict, List

from functional import seq
from pymongo import ASCENDING, DESCENDING, MongoClient


class ProjektiZadatak:

    def __init__(self) -> None:
        self.client = MongoClient('mongodb://127.0.0.1:27017/?readPreference=primary&appname=MongoDB+Compass&directConnection=true&ssl=false')
        self.collection_osnovni = self.client['projekt']['osnovni_dokumet']
        self.collection_decimal = self.client['projekt']['osnovni_decimal']
        self.statistika_osnovni_dokumnet = "statistika_osnovni_dokument"
        self.frekvencija_osnovni_dokumnet = ""

    def find_missig_fileds(self):
        missing_values = []
        collection = self.client['projekt']['osnovni_dokument']
        for parameter in collection.find_one():
            none_num = collection.count_documents({parameter: None})
            empty_str_num = collection.count_documents({parameter: ""})
            if empty_str_num != 0 or none_num != 0:
                missing_values.append(parameter, none_num, empty_str_num)
                print(f"{parameter} column with null values or emty string is {none_num + empty_str_num}")
        if  len(missing_values) == 0:
            print('No values with emty string or null values')


    def convert_to_float(self):
        start = time.time()
        atributes = self.client['projekt']['osnovni_dokument'].find_one()
        for key in atributes:
            if type(atributes[key]) == str:
                value = atributes[key].replace(".", "")
                if value.isdigit():
                    self.client['projekt']["osnovni_decimal"].update_many({key : {"$type": 2}},\
                         [{"$set": {key: {"$toDouble": f"${key}"}}}])
        end = time.time()
        print(f"Covert to float finished, time: {end - start}")


    def create_statistic_collection(self, collection_name: str):

        start = time.time()
        self.statistika_osnovni_dokumnet = collection_name
        row = self.collection_decimal.find_one()
        for col in row:
            if type(row[col]) == float:
                avg = list(self.collection_decimal.aggregate\
                    ([{"$group": {"_id": "$null", "av" : {"$avg": f"${col}"}}}]))
                std = list(self.collection_decimal.aggregate\
                    ([{"$group": { "_id": "$null", "std" : {"$stdDevPop": f"${col}"}}}]))
                self.client['projekt'][collection_name].insert_one(
                    {"Varijabla": col,
                     "Srednja vrijednost": avg[0]["av"],
                     "Standardna devijacija": std[0]["std"],
                     "Broj nomissing elemenata": 0})
        end = time.time()
        print(f"Creating statistic collection finished, time: {end - start}")


    def create_frequency_collection(self, collection_name: str):
        start = time.time()
        self.frekvencija_osnovni_dokumnet = collection_name
        categorical_col = ["codec", "o_codec"]
        for col in categorical_col:
            self.client['projekt'][collection_name].insert_one({"Varijabla": col})
            diff_values = self.collection_decimal.distinct(col) #  find all diffrent values in column
            for value in diff_values:
                freq = list(self.collection_decimal.aggregate([{"$match": {col: value}}, {"$count": col} ]))
                freq = freq[0][col]
                self.client['projekt'][collection_name].update_one({"Varijabla": col},\
                    { "$inc": { f"Pojavnost[{value}]": freq }})
        end = time.time()
        print(f"Creating frequency collection finished, time: {end - start}")


    def create_greater_or_less_then_mean(self, greater: bool, collection_name: str):

        start = time.time()
        operation = "$gt" if greater else "$lte"
        statistic_field = self.client['projekt'][self.statistika_osnovni_dokumnet].find()
        for value in statistic_field:
            greater_then_mean = list(self.collection_decimal.aggregate\
                ([ {'$match': {  value['Varijabla']: { operation:value['Srednja vrijednost']}}},
                {'$project':{value['Varijabla']: 1}}]))
            greater_then_mean = seq(greater_then_mean).map(lambda v: {'element_name': value['Varijabla'],
                                                                      'value': v[value['Varijabla']],
                                                                      'mean_value' : value['Srednja vrijednost']}).\
                                                                        to_list()
            if greater_then_mean:
                self.client['projekt'][collection_name].insert_many(greater_then_mean)
        end = time.time()
        print(f"Creating create_greater_or_less_then_mean collection finished, time: {end - start}")

    def embed_frequency_value(self, collection_name: str):

        start = time.time()
        codec = self.client['projekt']['frekvencija_osnovni_dokument'].find_one({"Varijabla": "codec"})
        o_codec = self.client['projekt']['frekvencija_podaci2'].find_one({"Varijabla": "o_codec"})
        collection = list(self.client['projekt']['osnovni_dokument'].find())
        for col in collection:
            col["codec"] = {"codec": {"value": col["codec"], "frekvencije": codec}}
            col["o_codec"] = {"o_codec": {"value": col["o_codec"], "frekvencije": o_codec}}
        self.client['projekt'][collection_name].insert_many(collection)

       #bez for petlje?
       # client['projekt'][collection_name].update_many({}, { '$set' : {"codec": {"codec": "$codec"  , "freq": codec} , "o_codec": o_codec }})
       # client['projekt'][collection_name].update_many({}, { '$set' : {"codec":  {"value" : {"$getField": "$codec" } } } })
        end = time.time()
        print(f"Creating embed_frequency_value collection finished, time: {end - start}")


    def embed_statistic_value(self, collection_name: str):

        start = time.time()
        statistic_collection = list(self.client['projekt']['statistika2_osnovni'].find())
        set_dict = {}
        for doc in statistic_collection:
            set_dict[doc['element_name']] = doc
        collection = list(self.client['projekt']['osnovni_dokument'].find())
        for col in collection:
            for param in set_dict:
                col[param] = {"value": col[param], "statistika": set_dict[param]}
        self.client['projekt'][collection_name].insert_many(collection)
        end = time.time()
        print(f"Creating embed_statistic_value collection finished, time: {end - start}")

    def remove_less_then_stdev(self, collection_name: str):

        start = time.time()
        diff_variables = list(self.client['projekt']['statistika_osnovni_dokument'].find())
        diff_variables_std = seq(diff_variables).\
            filter(lambda r: r['Standardna devijacija'] >\
                (r['Srednja vrijednost'] + r['Srednja vrijednost'] * 0.1)).to_list()
        unset_dist = {}
        for var in diff_variables_std:
            unset_dist[var["Varijabla"]] = ""
        self.client['projekt'][collection_name].update_many({},  { "$unset": unset_dist})
        end = time.time()
        print(f"remove_less_then_stdev finished, time: {end - start}")

    def compound_index(self, collection_name: str):

        start = time.time()
        self.client['projekt']['osnovni_dokument'].create_index([("bitrate", ASCENDING), ("utime", DESCENDING)])
        collection = self.client['projekt']['osnovni_dokument'].find().sort([("bitrate", ASCENDING), ("utime", DESCENDING)])
        self.client['projekt'][collection_name].insert_many(collection)
        end = time.time()
        print(f"compound_index finished, time: {end - start}")


if __name__ == "__main__":

    projekt = ProjektiZadatak()

    # Zadatak 1.
    projekt.find_missig_fileds()

    # Zadatak 2.
    projekt.convert_to_float()
    projekt.create_statistic_collection("statistika_osnovni_dokument")

    # Zadatak 3
    projekt.create_frequency_collection("frekvencija_osnovni_dokument")

    # Zadatak 4
    # greater then mean
    projekt.create_greater_or_less_then_mean(True, "statistika1_osnovni")

    # less or equal then mean
    projekt.create_greater_or_less_then_mean(False, "statistika2_osnovni")

    # Zadatak 5
    projekt.embed_frequency_value("emb1_osnovni_dokumnet")

    # Zadatak 6
    projekt.embed_statistic_value("emb2_osonovni_dokument")

    #Zadatak 7
    projekt.remove_less_then_stdev("emb2_osonovni_dokument")

    #Zadatak 8
    projekt.compound_index("slozeni_index")




















