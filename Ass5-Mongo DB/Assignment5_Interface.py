#
# Assignment5 Interface
# Name: Subhash Chandra Bose Vuppala
#

from pymongo import MongoClient
import os
import sys
import json
from math import cos, sin, sqrt, atan2, radians


def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    business_docs = collection.find({'city': {'$regex': "^" + cityToSearch + "$", '$options': "i"}})
    with open(saveLocation1, "w") as file:
        for business in business_docs:
            name = business['name']
            full_address = business['full_address'].replace("\n", ", ")
            city = business['city']
            state = business['state']
            file.write(name.upper() + "$" + full_address.upper() + "$" + city.upper() + "$" + state.upper() + "\n")


def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    business_docs = collection.find({'categories': {'$in': categoriesToSearch}},
                                    {'name': 1, 'latitude': 1, 'longitude': 1, 'categories': 1})
    latitude1 = float(myLocation[0])
    longitude1 = float(myLocation[1])
    with open(saveLocation2, "w") as file:
        for business in business_docs:
            name = business['name']
            latitude2 = float(business['latitude'])
            longitude2 = float(business['longitude'])
            dist = dist_funct(latitude2, longitude2, latitude1, longitude1)
            if dist <= maxDistance:
                file.write(name.upper() + "\n")


def dist_funct(latitude2, longitude2, latitude1, longitude1):
    radius = 3959
    pi_1 = radians(latitude1)
    pi_2 = radians(latitude2)
    delta_pi = radians(latitude2 - latitude1)
    delta_lambda = radians(longitude2 - longitude1)
    a = (sin(delta_pi / 2) * sin(delta_pi / 2)) + (
            cos(pi_1) * cos(pi_2) * sin(delta_lambda / 2) * sin(delta_lambda / 2))
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    d = radius * c

    return d
