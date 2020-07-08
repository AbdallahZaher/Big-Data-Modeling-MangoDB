import pandas as pd
from pymongo import MongoClient
from backend.DB import eu
from backend.DB import db

########################################################################################################################
countries = ['NO', 'HR', 'HU', 'CH', 'CZ', 'RO', 'LV', 'GR', 'UK', 'SI', 'LT',
             'ES', 'FR', 'IE', 'SE', 'NL', 'PT', 'PL', 'DK', 'MK', 'DE', 'IT',
             'BG', 'CY', 'AT', 'LU', 'BE', 'FI', 'EE', 'SK', 'MT', 'LI', 'IS']


def ex0_cpv_example(bot_year=2008, top_year=2020):
    """
    Returns all contracts in given year 'YEAR' range and cap to 100000000 the 'EURO_VALUE'

    Expected Output (list of documents):
    [{'result': count_value(int)}]
    """

    def year_filter(bot_year, top_year):
        filter_ = {
            '$match': {
                '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                'VALUE_EURO': {'$lt': 100000000}
            }}

        return filter_

    count = {
        '$count': 'result'
    }

    pipeline = [year_filter(bot_year, top_year), count]

    list_documents = list(eu.aggregate(pipeline))

    return list_documents


def ex1_cpv_box(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns five metrics, described below
    Result filterable by floor year, roof year and country_list

    Expected Output:
    (avg_cpv_euro_avg, avg_cpv_count, avg_cpv_offer_avg, avg_cpv_euro_avg_y_eu, avg_cpv_euro_avg_n_eu)

    Where:
    avg_cpv_euro_avg = average value of each CPV's division contracts average 'EURO_VALUE', (int)
    avg_cpv_count = average value of each CPV's division contract count, (int)
    avg_cpv_offer_avg = average value of each CPV's division contracts average NUMBER_OFFERS', (int)
    avg_cpv_euro_avg_y_eu = average value of each CPV's division contracts average EURO_VALUE' with 'B_EU_FUNDS', (int)
    avg_cpv_euro_avg_n_eu = average value of each CPV's division contracts average 'EURO_VALUE' with out 'B_EU_FUNDS' (int)
    """

    filter_ = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}, {'ISO_COUNTRY_CODE': {'$in': country_list}}]
        }}

    avg_cpv_euro_avg = int(list(
        db.eu.aggregate([filter_,
            {
                '$group':{
                    '_id':{
                        'CPV': { "$substr": [ "$CPV", 0, 2 ] }
                    },
                    'AVG':{'$avg':'$VALUE_EURO'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])
    
    avg_cpv_count = int(list(
        db.eu.aggregate([filter_,
            {
                '$group':{
                    '_id':{
                        'CPV': { "$substr": [ "$CPV", 0, 2 ] }
                    },
                    'SUM':{'$sum':1}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$SUM'}
                }
            },
            
        ])
    )[0]['Average'])

    avg_cpv_offer_avg = int(list(
        db.eu.aggregate([filter_,
            {
                '$group':{
                    '_id':{
                        'CPV': { "$substr": [ "$CPV", 0, 2 ] }
                    },
                    'AVG':{'$avg':'$NUMBER_OFFERS'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])


    avg_cpv_euro_avg_y_eu = int(list(
        db.eu.aggregate([filter_,
        {
            '$match':{
            '$and': [{'B_EU_FUNDS':'Y'}]
            }
            
        },
            {
                '$group':{
                    '_id':{
                        'CPV': { "$substr": [ "$CPV", 0, 2 ] }
                    },
                    'AVG':{'$avg':'$VALUE_EURO'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])



    
    avg_cpv_euro_avg_n_eu = int(list(
        db.eu.aggregate([filter_,
        {
            '$match':{
            '$and': [{'B_EU_FUNDS':'N'}]
            }
            
        },
            {
                '$group':{
                    '_id':{
                        'CPV': { "$substr": [ "$CPV", 0, 2 ] }
                    },
                    'AVG':{'$avg':'$VALUE_EURO'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])

    return avg_cpv_euro_avg, avg_cpv_count, avg_cpv_offer_avg, avg_cpv_euro_avg_y_eu, avg_cpv_euro_avg_n_eu


def ex2_cpv_treemap(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the count of contracts for each CPV Division
    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{cpv: value_1, count: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = contract count of each CPV Division, (int)
    """
    
    filter_ = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}, {'ISO_COUNTRY_CODE': {'$in': country_list}}]
        }}
    
    projection = {
        '$project':{
            '_id' : False,
            'CPV_DIVISION': {'$substr': ['$CPV', 0,2]}
        }
    }

    groupby_cpv_count = {
        '$group':{
            '_id': '$CPV_DIVISION',
            'count': {'$sum' : 1}
        }
    }

    lookup = {
        '$lookup': {
            'from': 'cpv',
            'foreignField': 'cpv_division',
            'localField': '_id',
            'as': 'CPV'
        }
    }

    projection2 = {
        '$project': {
            '_id' : False,
            'cpv':{ '$arrayElemAt': ['$CPV', 0]},
            'count':'$count'
        }
    }

    projection3 = {
        '$project': {
            'cpv': '$cpv.cpv_division_description',
            'count': True
        }
    }
    pipeline = [filter_, projection, groupby_cpv_count, lookup, projection2, projection3]

    list_documents = list(db.eu.aggregate(pipeline))
    
    return list_documents


def ex3_cpv_bar_1(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'EURO_VALUE' return the highest 5 cpvs
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'EURO_VALUE' of each CPV Division, (float)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}
                }
    }

    projection = {
        '$project':{
            '_id' : False,
            'CPV_DIVISION': {'$substr': ['$CPV', 0,2]},
            'VALUE_EURO': '$VALUE_EURO'
        }
    }

    groupby_cpv_count = {
        '$group':{
            '_id': '$CPV_DIVISION',
            'average': {'$avg' : '$VALUE_EURO'}
        }
    }

    lookup = {
        '$lookup': {
            'from': 'cpv',
            'foreignField': 'cpv_division',
            'localField': '_id',
            'as': 'CPV'
        }
    }

    projection2 = {
        '$project': {
            '_id' : False,
            'cpv':{ '$arrayElemAt': ['$CPV', 0]},
            'avg':'$average'
        }
    }

    projection3 = {
        '$project': {
            'cpv': '$cpv.cpv_division_description',
            'avg': True
        }
    }

    sort = {
        '$sort':{
            'avg':-1
        }
    }
    limit = {
        '$limit':5
        }
    
    pipeline = [filter_, projection, groupby_cpv_count, lookup, projection2, projection3, sort, limit]

    list_documents = list(db.eu.aggregate(pipeline))
    
    return list_documents


def ex4_cpv_bar_2(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'EURO_VALUE' return the lowest 5 cpvs
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'EURO_VALUE' of each CPV Division, (float)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}
                }
    }

    projection = {
        '$project':{
            '_id' : False,
            'CPV_DIVISION': {'$substr': ['$CPV', 0,2]},
            'VALUE_EURO': '$VALUE_EURO'
        }
    }

    groupby_cpv_count = {
        '$group':{
            '_id': '$CPV_DIVISION',
            'average': {'$avg' : '$VALUE_EURO'}
        }
    }

    lookup = {
        '$lookup': {
            'from': 'cpv',
            'foreignField': 'cpv_division',
            'localField': '_id',
            'as': 'CPV'
        }
    }

    projection2 = {
        '$project': {
            '_id' : False,
            'cpv':{ '$arrayElemAt': ['$CPV', 0]},
            'avg':'$average'
        }
    }

    projection3 = {
        '$project': {
            'cpv': '$cpv.cpv_division_description',
            'avg': True
        }
    }

    sort = {
        '$sort':{
            'avg':1
        }
    }
    limit = {
        '$limit':5
        }
    
    pipeline = [filter_, projection, groupby_cpv_count, lookup, projection2, projection3, sort, limit]

    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents


def ex5_cpv_bar_3(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'EURO_VALUE' return the highest 5 cpvs for contracts which recieved european funds ('B_EU_FUNDS') 
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'EURO_VALUE' of each CPV Division, (float)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}, 'B_EU_FUNDS':'Y'
                }
    }

    projection = {
        '$project':{
            '_id' : False,
            'CPV_DIVISION': {'$substr': ['$CPV', 0,2]},
            'VALUE_EURO': '$VALUE_EURO'
        }
    }

    groupby_cpv_count = {
        '$group':{
            '_id': '$CPV_DIVISION',
            'average': {'$avg' : '$VALUE_EURO'}
        }
    }

    lookup = {
        '$lookup': {
            'from': 'cpv',
            'foreignField': 'cpv_division',
            'localField': '_id',
            'as': 'CPV'
        }
    }

    projection2 = {
        '$project': {
            '_id' : False,
            'cpv':{ '$arrayElemAt': ['$CPV', 0]},
            'avg':'$average'
        }
    }

    projection3 = {
        '$project': {
            'cpv': '$cpv.cpv_division_description',
            'avg': True
        }
    }

    sort = {
        '$sort':{
            'avg':-1
        }
    }
    limit = {
        '$limit':5
        }
    
    pipeline = [filter_, projection, groupby_cpv_count, lookup, projection2, projection3, sort, limit]

    list_documents = list(db.eu.aggregate(pipeline))
    
    return list_documents


def ex6_cpv_bar_4(bot_year=2008, top_year=2020, country_list=countries):
    """
    Per CPV Division and get the average 'EURO_VALUE' return the highest 5 cpvs for contracts which did not recieve european funds ('B_EU_FUNDS') 
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{cpv: value_1, avg: value_2}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'EURO_VALUE' of each CPV Division, (float)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}, 'B_EU_FUNDS':'N'
                }
    }

    projection = {
        '$project':{
            '_id' : False,
            'CPV_DIVISION': {'$substr': ['$CPV', 0,2]},
            'VALUE_EURO': '$VALUE_EURO'
        }
    }

    groupby_cpv_count = {
        '$group':{
            '_id': '$CPV_DIVISION',
            'average': {'$avg' : '$VALUE_EURO'}
        }
    }

    lookup = {
        '$lookup': {
            'from': 'cpv',
            'foreignField': 'cpv_division',
            'localField': '_id',
            'as': 'CPV'
        }
    }

    projection2 = {
        '$project': {
            '_id' : False,
            'cpv':{ '$arrayElemAt': ['$CPV', 0]},
            'avg':'$average'
        }
    }

    projection3 = {
        '$project': {
            'cpv': '$cpv.cpv_division_description',
            'avg': True
        }
    }

    sort = {
        '$sort':{
            'avg':-1
        }
    }
    limit = {
        '$limit':5
        }
    
    pipeline = [filter_, projection, groupby_cpv_count, lookup, projection2, projection3, sort, limit]

    list_documents = list(db.eu.aggregate(pipeline))
    
    return list_documents


def ex7_cpv_map(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the highest CPV Division on average 'EURO_VALUE' per country 'ISO_COUNTRY_CODE'

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{cpv: value_1, avg: value_2, country: value_3}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = highest CPV Division average 'EURO_VALUE' of country, (float)
    value_3 = country in ISO-A2 format (string) (located in iso_codes collection)
    """
    
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}, 'ISO_COUNTRY_CODE': {'$ne': None}, 'CPV': {'$ne': None}
                }
    }

    groupby_isocode_sum = {
                '$group':{
                    '_id':{'iso': '$ISO_COUNTRY_CODE', 'cpv':{'$substr': ['$CPV', 0,2]}},
                    'avg': {'$avg' : '$VALUE_EURO'}
                }
                    
    }

    sort = { "$sort": 
                { "_id.iso": 1, 
                 "avg": -1 
                } 
    }
        

    groupby_isocode_max = {    
        "$group": {
            '_id':'$_id.iso',
            "avg": {"$first": "$avg"},
            "cpv": {"$first": "$_id.cpv"}
            }
    }


    lookup = {
        '$lookup': {
            'from': 'iso_codes',
            'foreignField': 'alpha-2',
            'localField': '_id',
            'as': 'ISOCODES'
        }
    }


    projection1 = {
        '$project': {
            '_id' : '$_id',
            'isocodes':{ '$arrayElemAt': ['$ISOCODES', 0]},
            'avg':'$avg',
            'cpv': '$cpv'
        }
    }

    projection2 = {
        '$project': {
            '_id': False,
            'country': '$isocodes.name',
            'avg': True,
            'cpv': '$cpv'
        }
    }


    pipeline = [filter_,groupby_isocode_sum,sort,groupby_isocode_max,lookup,projection1,projection2]

    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents


def ex8_cpv_hist(bot_year=2008, top_year=2020, country_list=countries, cpv='50'):
    """
    Produce an histogram where each bucket has the contract counts of a particular cpv
     in a given range of values (bucket) according to 'EURO_VALUE'

     Choose 10 buckets of any partition
    Buckets Example:
     0 to 100000
     100000 to 200000
     200000 to 300000
     300000 to 400000
     400000 to 500000
     500000 to 600000
     600000 to 700000
     700000 to 800000
     800000 to 900000
     900000 to 1000000


    So given a CPV Division code (two digit string) return a list of documents where each document as the bucket _id,
    and respective bucket count.

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{bucket: value_1, count: value_2}, ....]

    Where:
    value_1 = lower limit of respective bucket (if bucket position 0 of example then bucket:0 )
    value_2 = contract count for thar particular bucket, (int)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}, "$expr":{"$eq":[{'$substr': ['$CPV', 0,2]}, cpv]},
                    'VALUE_EURO': {'$ne': None}
                    
                }
    }

    projection = {
        '$project':{
            '_id' : False,
            'CPV_DIVISION': {'$substr': ['$CPV', 0,2]},
            'value_euro': "$VALUE_EURO" 
        }
    }

    bucket = {
        '$bucket': {
          'groupBy': "$value_euro",                        
          'boundaries': [0, 100000, 200000, 300000, 400000, 500000, 600000, 700000, 800000, 900000, 1000000], 
          'default': ">1000000"
        }
    }

    projection2 = {
        '$project': {
            '_id' : False,
            'bucket':'$_id',
            'count':'$count'
        }
    }

    pipeline = [filter_, projection, bucket, projection2]

    list_documents = list(db.eu.aggregate(pipeline))
    
    return list_documents  



def ex9_cpv_bar_diff(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average time and value difference for each CPV, return the highest 5 cpvs

    time difference = 'DT-DISPATCH' - 'DT-AWARD'
    value difference = 'AWARD_VALUE_EURO' - 'EURO_VALUE'

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{cpv: value_1, time_difference: value_2, value_difference: value_3}, ....]

    Where:
    value_1 = CPV Division description, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'DT-DISPACH' - 'DT-AWARD', (float)
    value_3 = average 'EURO_AWARD' - 'EURO_VALUE' (float)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}, 'DT_DISPATCH': {'$ne': None}, 'DT_AWARD': {'$ne': None}, 'CPV': {'$ne': None}
                }
    }

    projection = {
        '$project':{
            '_id' : False,
            'CPV_DIVISION': {'$substr': ['$CPV', 0,2]},
            'time_difference': {'$subtract' : [ {'$dateFromString': { 'dateString': '$DT_DISPATCH'}}, {'$dateFromString': { 'dateString': '$DT_AWARD'}} ]},
            'value_difference': {'$subtract' : [ "$AWARD_VALUE_EURO", "$VALUE_EURO" ]}
        }
    }

    groupby_cpv_count = {
        '$group':{
            '_id': '$CPV_DIVISION',
            'time_difference': {'$avg' : '$time_difference'},
            'value_difference': {'$avg' : '$value_difference' }
        }
    }

    lookup = {
        '$lookup': {
            'from': 'cpv',
            'foreignField': 'cpv_division',
            'localField': '_id',
            'as': 'CPV'
        }
    }

    projection2 = {
        '$project': {
            '_id' : False,
            'cpv':{ '$arrayElemAt': ['$CPV', 0]},
            'time_difference':'$time_difference',
            'value_difference':'$value_difference'
        }
    }

    projection3 = {
        '$project': {
            'cpv': '$cpv.cpv_division_description',
            'time_difference':True,
            'value_difference':True
        }
    }

    sort = {
        '$sort':{
            'time_difference':-1
            
        }
    }

    limit = {'$limit':5}


    pipeline = [filter_, projection, groupby_cpv_count, lookup, projection2, projection3, sort, limit]

    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents


def ex10_country_box(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want five numbers, described below
    Result filterable by floor year, roof year and country_list

    Expected Output:
    (avg_country_euro_avg, avg_country_count, avg_country_offer_avg, avg_country_euro_avg_y_eu, avg_country_euro_avg_n_eu)

    Where:
    avg_country_euro_avg = average value of each countries ('ISO_COUNTRY_CODE') contracts average 'EURO_VALUE', (int)
    avg_country_count = average value of each countries ('ISO_COUNTRY_CODE') contract count, (int)
    avg_country_offer_avg = average value of each countries ('ISO_COUNTRY_CODE') contracts average NUMBER_OFFERS', (int)
    avg_country_euro_avg_y_eu = average value of each countries ('ISO_COUNTRY_CODE') contracts average EURO_VALUE' with 'B_EU_FUNDS', (int)
    avg_country_euro_avg_n_eu = average value of each countries ('ISO_COUNTRY_CODE') contracts average 'EURO_VALUE' with out 'B_EU_FUNDS' (int)
    """


    filter_ = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}, {'ISO_COUNTRY_CODE': {'$in': country_list}}]
        }}

    avg_country_euro_avg = int(list(
        db.eu.aggregate([filter_,
            {
                '$group':{
                    '_id':{
                        'Country': '$ISO_COUNTRY_CODE'
                    },
                    'AVG':{'$avg':'$VALUE_EURO'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])



    avg_country_count = int(list(
        db.eu.aggregate([filter_,
            {
                '$group':{
                    '_id':{
                        'Country': '$ISO_COUNTRY_CODE'
                    },
                    'SUM':{'$sum':1}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$SUM'}
                }
            },
            
        ])
    )[0]['Average'])

    avg_country_offer_avg = int(list(
        db.eu.aggregate([filter_,
            {
                '$group':{
                    '_id':{
                        'Country': '$ISO_COUNTRY_CODE'
                    },
                    'AVG':{'$avg':'$NUMBER_OFFERS'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])


    avg_country_euro_avg_y_eu = int(list(
        db.eu.aggregate([filter_,
        {
            '$match':{
            '$and': [{'B_EU_FUNDS':'Y'}]
            }
            
        },
            {
                '$group':{
                    '_id':{
                        'Country': '$ISO_COUNTRY_CODE'
                    },
                    'AVG':{'$avg':'$VALUE_EURO'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])



    
    avg_country_euro_avg_n_eu = int(list(
        db.eu.aggregate([filter_,
        {
            '$match':{
            '$and': [{'B_EU_FUNDS':'N'}]
            }
            
        },
            {
                '$group':{
                    '_id':{
                        'Country': '$ISO_COUNTRY_CODE'
                    },
                    'AVG':{'$avg':'$VALUE_EURO'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])

    return avg_country_euro_avg, avg_country_count, avg_country_offer_avg, avg_country_euro_avg_y_eu, avg_country_euro_avg_n_eu


def ex11_country_treemap(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the count of contracts per country ('ISO_COUNTRY_CODE')
    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{country: value_1, count: value_2}, ....]

    Where:
    value_1 = Country ('ISO_COUNTRY_CODE') name, (string) (located in iso_codes collection')
    value_2 = contract count of each country, (int)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}
                }
    }

    groupby_isocode_count = {
                '$group':{
                    '_id':'$ISO_COUNTRY_CODE',
                    'count': {'$sum' : 1}
                }
    }

    lookup = {
        '$lookup': {
            'from': 'iso_codes',
            'foreignField': 'alpha-2',
            'localField': '_id',
            'as': 'ISOCODES'
        }
    }

    projection1 = {
        '$project': {
            '_id' : False,
            'isocodes':{ '$arrayElemAt': ['$ISOCODES', 0]},
            'count':'$count'
        }
    }

    projection2 = {
        '$project': {
            'country': '$isocodes.name',
            'count': True
        }
    }

    pipeline = [filter_,groupby_isocode_count,lookup,projection1,projection2]

    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents


def ex12_country_bar_1(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average 'EURO_VALUE' for each country, return the highest 5 countries

    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{country: value_1, avg: value_2}, ....]

    Where:
    value_1 = Country ('ISO_COUNTRY_CODE') name, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'EURO_VALUE' of each country ('ISO_COUNTRY_CODE') name, (float)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}
                }
    }

    groupby_isocode_count = {
                '$group':{
                    '_id':'$ISO_COUNTRY_CODE',
                    'average': {'$avg' : '$VALUE_EURO'}
                }
    }

    lookup = {
        '$lookup': {
            'from': 'iso_codes',
            'foreignField': 'alpha-2',
            'localField': '_id',
            'as': 'ISOCODES'
        }
    }

    projection1 = {
        '$project': {
            '_id' : False,
            'isocodes':{ '$arrayElemAt': ['$ISOCODES', 0]},
            'avg':'$average'
        }
    }

    projection2 = {
        '$project': {
            'country': '$isocodes.name',
            'avg': True
        }
    }

    sort = {
        '$sort':{
            'avg':-1
        }
    }
    limit = {'$limit':5}


    pipeline = [filter_,groupby_isocode_count,lookup,projection1,projection2,sort,limit]

    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents


def ex13_country_bar_2(bot_year=2008, top_year=2020, country_list=countries):
    """
    Group by country and get the average 'EURO_VALUE' for each group, return the lowest, average wise, 5 documents

    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{country: value_1, avg: value_2}, ....]

    Where:
    value_1 = Country ('ISO_COUNTRY_CODE') name, (string) (located in cpv collection as 'cpv_division_description')
    value_2 = average 'EURO_VALUE' of each country ('ISO_COUNTRY_CODE') name, (float)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}
                }
    }

    groupby_isocode_count = {
                '$group':{
                    '_id':'$ISO_COUNTRY_CODE',
                    'average': {'$avg' : '$VALUE_EURO'}
                }
    }

    lookup = {
        '$lookup': {
            'from': 'iso_codes',
            'foreignField': 'alpha-2',
            'localField': '_id',
            'as': 'ISOCODES'
        }
    }

    projection1 = {
        '$project': {
            '_id' : False,
            'isocodes':{ '$arrayElemAt': ['$ISOCODES', 0]},
            'avg':'$average'
        }
    }

    projection2 = {
        '$project': {
            'country': '$isocodes.name',
            'avg': True
        }
    }

    sort = {
        '$sort':{
            'avg':1
        }
    }
    limit = {'$limit':5}


    pipeline = [filter_,groupby_isocode_count,lookup,projection1,projection2,sort,limit]

    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents


def ex14_country_map(bot_year=2008, top_year=2020, country_list=countries):
    """
    For each country get the sum of thr«e respective contracts 'EURO_VALUE'

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{sum: value_1, country: value_2}, ....]

    Where:
    value_1 = sum 'EURO_VALUE' of country ('ISO_COUNTRY_CODE') name, (float)
    value_2 = country in ISO-A2 format (string) (located in iso_codes collection)
    """
    filter_ = {
            '$match': {
                '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                'ISO_COUNTRY_CODE': {'$in': country_list}, 'B_EU_FUNDS':'Y'
            }
    }

    groupby_isocode_count = {
                '$group':{
                    '_id':'$ISO_COUNTRY_CODE',
                    'sum': {'$sum' : '$VALUE_EURO'}
                }
    }

    lookup = {
        '$lookup': {
            'from': 'iso_codes',
            'foreignField': 'alpha-2',
            'localField': '_id',
            'as': 'ISOCODES'
        }
    }

    projection1 = {
        '$project': {
            '_id' : False,
            'isocodes':{ '$arrayElemAt': ['$ISOCODES', 0]},
            'sum':'$sum'
        }
    }

    projection2 = {
        '$project': {
            'country': '$isocodes.name',
            'sum': True
        }
    }


    pipeline = [filter_,groupby_isocode_count,lookup,projection1,projection2]

    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents


def ex15_business_box(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want five numbers, described below

    Result filterable by floor year, roof year and country_list

    Expected Output:
    (avg_business_euro_avg, avg_business_count, avg_business_offer_avg, avg_business_euro_avg_y_eu, avg_business_euro_avg_n_eu)

    Where:
    avg_business_euro_avg = average value of each company ('CAE_NAME')  contracts average 'EURO_VALUE', (int)
    avg_business_count = average value of each company ('CAE_NAME') contract count, (int)
    avg_business_offer_avg = average value of each company ('CAE_NAME') contracts average NUMBER_OFFERS', (int)
    avg_business_euro_avg_y_eu = average value of each company ('CAE_NAME') contracts average EURO_VALUE' with 'B_EU_FUNDS', (int)
    avg_business_euro_avg_n_eu = average value of each company ('CAE_NAME') contracts average 'EURO_VALUE' with out 'B_EU_FUNDS' (int)
    """

    filter_ = {
        '$match': {
            '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}, {'ISO_COUNTRY_CODE': {'$in': country_list}}]
        }}

    avg_business_euro_avg = int(list(
        db.eu.aggregate([filter_,
            {
                '$group':{
                    '_id':{
                        'Company': '$CAE_NAME'
                    },
                    'AVG':{'$avg':'$VALUE_EURO'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])



    avg_business_count = int(list(
        db.eu.aggregate([filter_,
            {
                '$group':{
                    '_id':{
                        'Company': '$CAE_NAME'
                    },
                    'SUM':{'$sum':1}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$SUM'}
                }
            },
            
        ])
    )[0]['Average'])

    avg_business_offer_avg = int(list(
        db.eu.aggregate([filter_,
            {
                '$group':{
                    '_id':{
                        'Company': '$CAE_NAME'
                    },
                    'AVG':{'$avg':'$NUMBER_OFFERS'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])


    avg_business_euro_avg_y_eu = int(list(
        db.eu.aggregate([filter_,
        {
            '$match':{
            '$and': [{'B_EU_FUNDS':'Y'}]
            }
            
        },
            {
                '$group':{
                    '_id':{
                        'Company': '$CAE_NAME'
                    },
                    'AVG':{'$avg':'$VALUE_EURO'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])



    
    avg_business_euro_avg_n_eu = int(list(
        db.eu.aggregate([filter_,
        {
            '$match':{
            '$and': [{'B_EU_FUNDS':'N'}]
            }
            
        },
            {
                '$group':{
                    '_id':{
                        'Company': '$CAE_NAME'
                    },
                    'AVG':{'$avg':'$VALUE_EURO'}
                }
            },
            
            {
                '$group':{
                    '_id': None,
                    'Average':{'$avg':'$AVG'}
                }
            },
            
        ])
    )[0]['Average'])


    return avg_business_euro_avg, avg_business_count, avg_business_offer_avg, avg_business_euro_avg_y_eu, avg_business_euro_avg_n_eu


def ex16_business_bar_1(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average 'EURO_VALUE' for company ('CAE_NAME') return the highest 5 companies
    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{company: value_1, avg: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME') name, (string)
    value_2 = average 'EURO_VALUE' of each company ('CAE_NAME'), (float)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}
                }
    }

    groupby_cae_count = {
                '$group':{
                    '_id':'$CAE_NAME',
                    'avg': {'$avg' : '$VALUE_EURO'}
                }
    }

    projection = {
        '$project': {
            '_id' : False,
            'company': '$_id',
            'avg':'$avg'
        }
    }

    sort = {
        '$sort':{
            'avg':-1
        }
    }

    limit = {'$limit':5}
    
    pipeline = [filter_,groupby_cae_count,projection,sort,limit]
    
    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents


def ex17_business_bar_2(bot_year=2008, top_year=2020, country_list=countries):
    """
    Returns the average 'EURO_VALUE' for company ('CAE_NAME') return the lowest 5 companies


    Result filterable by floor year, roof year and country_list

    Expected Output (list of 5 sorted documents):
    [{company: value_1, avg: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME') name, (string)
    value_2 = average 'EURO_VALUE' of each company ('CAE_NAME'), (float)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}
                }
    }

    groupby_cae_count = {
                '$group':{
                    '_id':'$CAE_NAME',
                    'avg': {'$avg' : '$VALUE_EURO'}
                }
    }

    projection = {
        '$project': {
            '_id' : False,
            'company': '$_id',
            'avg':'$avg'
        }
    }

    sort = {
        '$sort':{
            'avg':1
        }
    }

    limit = {'$limit':5}
    
    pipeline = [filter_,groupby_cae_count,projection,sort,limit]
    
    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents


def ex18_business_treemap(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want the count of contracts for each company 'CAE_CODE', for the highest 15
    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{company: value_1, count: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME'), (string)
    value_2 = contract count of each company ('CAE_NAME'), (int)
    """
    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}
                }
    }

    groupby_cae_count = {
                '$group':{
                    '_id':'$CAE_NAME',
                    'count': {'$sum' : 1}
                }
    }

    projection = {
        '$project': {
            '_id' : False,
            'company': '$_id',
            'count':'$count'
        }
    }

    sort = {
        '$sort':{
            'count':-1
        }
    }

    limit = {'$limit':15}
    
    pipeline = [filter_,groupby_cae_count,projection,sort,limit]

    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents


def ex19_business_map(bot_year=2008, top_year=2020, country_list=countries):
    """
    For each country get the highest sum, in terms of 'EURO_VALUE', company ('CAE_NAME')

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{company: value_1, sum: value_2, country: value_3, address: value_4}, ....]

    Where:
    value_1 = 'top' company of that particular country ('CAE_NAME'), (string)
    value_2 = sum 'EURO_VALUE' of country and company ('CAE_NAME'), (float)
    value_3 = country in ISO-A2 format (string) (located in iso_codes collection)
    value_4 = company ('CAE_NAME') address, single string merging 'CAE_ADDRESS' and 'CAE_TOWN' separated by ' ' (space)
    """

    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}, 'CAE_ADDRESS': {'$ne': None}, 'CAE_TOWN': {'$ne': None}
                }
    }

    groupby_isocode_sum = {
                '$group':{
                    '_id':{'iso': '$ISO_COUNTRY_CODE', 'caename':'$CAE_NAME', 'address': {'$concat': [{'$toString': '$CAE_ADDRESS'},' ',{'$toString': '$CAE_TOWN'}]}},
                    'sum': {'$sum' : '$VALUE_EURO'}
                }
                    
    }

    sort = { '$sort': 
                { '_id.iso': 1, 
                 'sum': -1 
                } 
    }
        
    groupby_isocode_max = {    
        "$group": {
            '_id':'$_id.iso',
            'sum': {'$first': '$sum'},
            'caename': {'$first': '$_id.caename'},
            'address': {'$first': '$_id.address'}
            }
    }

    lookup = {
        '$lookup': {
            'from': 'iso_codes',
            'foreignField': 'alpha-2',
            'localField': '_id',
            'as': 'ISOCODES'
        }
    }

    projection1 = {
        '$project': {
            '_id' : '$_id',
            'isocodes':{'$arrayElemAt': ['$ISOCODES', 0]},
            'sum':'$sum',
            'company': '$caename',
            'address': '$address'
        }
    }

    projection2 = {
        '$project': {
            '_id': False,
            'address': True,
            'country': '$isocodes.name',
            'sum': True,
            'company': True
        }
    }


    pipeline = [filter_,groupby_isocode_sum,sort,groupby_isocode_max,lookup,projection1,projection2]

    list_documents = list(db.eu.aggregate(pipeline, allowDiskUse=True))

    return list_documents


def ex20_business_connection(bot_year=2008, top_year=2020, country_list=countries):
    """
    We want the top 5 most co-occurring companies ('CAE_NAME' and 'WIN_NAME')

    Result filterable by floor year, roof year and country_list

    Expected Output (list of documents):
    [{companies: value_1, count: value_2}, ....]

    Where:
    value_1 = company ('CAE_NAME') string merged with company ('WIN_NAME') seperated by the string ' with ', (string)
    value_2 = co-occurring number of contracts (int)
    """

    filter_ = {
                '$match': {
                    '$and': [{'YEAR': {'$gte': bot_year}}, {'YEAR': {'$lte': top_year}}],
                    'ISO_COUNTRY_CODE': {'$in': country_list}, 'CAE_ADDRESS': {'$ne': None}, 'WIN_NAME': {'$ne': None}
                }
    }

    groupby_companies_count = {
                '$group':{
                    '_id':{'$concat': [{'$toString': '$CAE_ADDRESS'},' with ',{'$toString': '$CAE_TOWN'}]},
                    'count': {'$sum' : 1}
                }
    }

    projection = {
        '$project': {
            '_id' : False,
            'companies': '$_id',
            'count':'$count'
        }
    }

    sort = {
        '$sort':{
            'count':-1
        }
    }

    limit = {'$limit':5}


    pipeline = [filter_,groupby_companies_count,projection,sort,limit]
    
    list_documents = list(db.eu.aggregate(pipeline))

    return list_documents

def insert_operation(document):
    '''
        Insert operation.

        In case pre computed tables were generated for the queries they should be recomputed with the new data.
    '''
    inserted_ids = eu.insert_many(document).inserted_ids

    return inserted_ids


query_list = [
    ex1_cpv_box, ex2_cpv_treemap, ex3_cpv_bar_1, ex4_cpv_bar_2,
    ex5_cpv_bar_3, ex6_cpv_bar_4, ex7_cpv_map, ex8_cpv_hist ,ex9_cpv_bar_diff,
    ex10_country_box, ex11_country_treemap, ex12_country_bar_1,
    ex13_country_bar_2, ex14_country_map, ex15_business_box,
    ex16_business_bar_1, ex17_business_bar_2, ex18_business_treemap,
    ex19_business_map, ex20_business_connection
]
