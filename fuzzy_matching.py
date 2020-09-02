"""
When imported, this script defines a namedtuple subclass called
Candidate and defines functions for matching raw addresses to parcels
in the jmb database.

"""

import re
from collections import namedtuple
import time
import datetime

Candidate = namedtuple('Candidate',
                       ['parcel_id',
                        'match_id',
                        'parcel_address1',
                        'parcel_address2',
                        'parcel_city',
                        'parcel_state',
                        'to_match_address',
                       ]
                       )


def print_now(string=''):
    global start
    try:
        start
    except NameError:
        start = datetime.datetime.now()
    global last_time
    try:
        last_time
    except NameError:
        last_time = datetime.datetime.now()
    total_time = datetime.datetime.now() - start
    delta_time = datetime.datetime.now() - last_time
    print(
        f'{datetime.datetime.now()} -- '
        f'{total_time} '
        f'-- last step took: '
        f'{delta_time}'
        f' {string}',
        flush=True)
    last_time = datetime.datetime.now()


def find_winner(candidate):
    """Determine if two addresses are perfect match or how similar they
    are.

    This function takes a namedtuple consisting of a potential match
    between one of our parcels and some other address. This function
     cleans the candidate parcel address and to_match address then
     compares them to see if there's a perfect match. If matched,
     return winner tuple, else return none. Function will also return
     None if there was no candidate passed or if candidate is empty.

    :param candidate: An instance of the Candidate class with 2
    addresses to compare and their respective ids.
    :param zip_code: Boolean to tell function whether or not to include
    zip codes as part of the matching comparison.
    :return: Winner tuple if perfect match, else none if no valid
    candidate was passed or there was no perfect match.
    """
    if candidate:
        clean_to_match = normalize_address(candidate.to_match_address)
        # if not zip_code:  # Whether or not to include zip_code in comparison
            # if re.match('\d{5}', candidate.to_match_address.split()[-1]):
            #     """Possible improvements on this line.
            #     \d{5} doesn't match a zip+4. Needs to refactor to include
            #     that possibility
            #     """
            #     clean_to_match = ' '.join(clean_to_match.split()[:-1])
        norm_parcel_address = normalize_address(
            ' '.join([candidate.parcel_address1,
                      candidate.parcel_address2,
                      candidate.parcel_city,
                      candidate.parcel_state]))
        """
        Use the following commented lines to approximate matches to 
        determine if the matching is working correctly. Set 
        breakpoint on ratio and run in debugger to compare ratio with 
        norm_parcel address and clean_to_match.
        """
        # ratio = fuzz.ratio(norm_parcel_address, clean_to_match)
        # if 95 < ratio < 100:
        #     ratio
        if norm_parcel_address == clean_to_match:
            winner = (
                '100',
                norm_parcel_address,
                clean_to_match,
                candidate.parcel_id,
                candidate.match_id,
                candidate.parcel_address1,
                candidate.parcel_address2,
                candidate.parcel_city,
                candidate.parcel_state,
                candidate.to_match_address
            )
            return winner
        return None
    else:
        return None


def normalize_address(input):
    """Transform input into a stardardized address string

    Transforms input into a normalized address string by splitting
    input on whitespace, and, for each component, eliminating
    non-alphanumeric characters, making component lowercase,
    transforming common address abbreviations into a standardized
    version, then joining all components back together into one string
    delimited by ' '.

    Join without the whitespace. It doesn't add any value and it
    prevents things like "bluff view" and "bluffview" from matching
    perfectly. Also would need to change the code for norm_candidate
    so they match.

    :param input: any string that represents a complete address or
    component of an address.
    :return: normalized address string for match comparison purposes
    only. Not formatted as a proper mailing address. Return None if no
    input was provided ('' or None).
    """
    if input:
        abbv = transformations()
        done_address = []
        if re.match('\d{5}', input.split()[-1]):
            """Possible improvements on this line.
            \d{5} doesn't match a zip+4. Needs to refactor to include 
            that possibility
            """
            address = ' '.join(input.split()[:-1])
        else:
            address = input
        address_components = address.split()
        for i, word in enumerate(address_components):
            word_after_sub = re.sub('[^0-9a-zA-Z]+', '', word).lower()
            if word_after_sub in abbv.keys():
                done_address.append(abbv[word_after_sub])
            else:
                done_address.append(word_after_sub)
        return ' '.join(x for x in done_address if x)
    else:
        return None


def transformations():
    """
    Known issues:
        Debating whether it's wise to turn some strange words like
        building into '' or just 'bldg' to limit their effect on
        throwing the ratio off. Not an issue if only using 100% matches
        for updates.

        Used to transform cardinal directions (N, E, S, W) to '' but
        don't anymore.

        Used to transform longer versions of components to a compact
        version (ie. street became st) but then switched to
        transforming to '' (ie. street became '').

        Transforming cardinal directions (ie. 'north': 'n') is
        dangerous if using Levenshtein ratio because 2 parcels may have
        the same ratio but be different addresses. For example,
        candidates ('400 s 200 e' and '400 n 200 e') would both have
        the same match ratio to '400 n 200 w' in the same city. Not an
         issue if only using 100% matches for updates.

        Cities like Saint George have many variations (Saint, St., St)
        and some of those variations are shared with the transformation
        for street (St., St). Changing all variations in the city name
        into '' is problematic because there could be a fort george in
        the same state with the same address. Extremely unlikely but
        possible. See note in populate_vacancy_parcel_id_notes.psql.

        Changing street prefix 'way' into wy or wy into '' is dangerous
        because of state abbreviations like wy for
        wyoming.


    :return: dictionary of keys that need to be transformed into values
    for standardization
    """
    return {
        'apt': '',
        'ave': '',
        'avenue': '',
        'av': '',
        'dr': '',
        'drive': '',
        'building': 'bldg',
        'blvd': '',
        'boulevard': '',
        'ct': '',
        'court': '',
        'circle': '',
        'cir': '',
        'east': 'e',
        'fort': 'ft',
        'highway': '',
        'parkway': '',
        'lane': '',
        'ln': '',
        'mount': 'mt',
        'num': '',
        'north': 'n',
        'pl': '',
        'place': '',
        'road': '',
        'rd': '',
        'saint': '',
        'st': '',
        'street': '',
        'south': 's',
        'ste': '',
        'suite': '',
        'str': '',
        'trail': '',
        'unit': '',
        'way': '',
        'terrace': '',
        'west': 'w',
        'wy': '',
        '1st': 'first',
        '2nd': 'second',
        '3rd': 'third',
        '4th': 'fourth',
        '5th': 'fifth',
        '6th': 'sixth',
        '7th': 'seventh',
        '8th': 'eighth',
        '9th': 'ninth',
        '10th': 'tenth',
        '11th': 'eleventh',
        '12th': 'twelfth',
        '13th': 'thirteenth',
        '14th': 'fourteenth',
        '15th': 'fifteenth',
        '16th': 'sixteenth',
    }
