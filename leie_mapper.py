#! /usr/bin/env python3

import sys
import os
import argparse
import csv
import json
import time
from datetime import datetime
from dateutil.parser import parse as dateparse
import signal
import random
import hashlib

#=========================
class mapper():

    #----------------------------------------
    def __init__(self):

        self.load_reference_data()
        self.stat_pack = {}

    #----------------------------------------
    def map(self, raw_data, input_row_num = None):
        json_data = {}

        #--clean values
        for attribute in raw_data:
            if attribute not in ('LASTNAME'): #--some people of the last name NULL
                raw_data[attribute] = self.clean_value(raw_data[attribute])

        #--place any filters needed here

        #--place any calculations needed here

        #--need a unique key
        pii_attrs = ['LASTNAME',
                     'FIRSTNAME',
                     'MIDNAME',
                     'BUSNAME',
                     'UPIN',
                     'NPI',
                     'DOB',
                     'ADDRESS',
                     'CITY',
                     'STATE',
                     'ZIP']
        record_id = self.compute_record_hash(raw_data, pii_attrs)

        #--determine if a person or organziation
        if raw_data['LASTNAME']:
            record_type = 'PERSON'
            if raw_data['BUSNAME']:
                self.update_stat('!INFO', 'BOTH_LAST_AND_BUS_NAME', input_row_num)
        elif raw_data['BUSNAME']:
            record_type = 'ORGANIZATION'
        else:
            record_type = 'PERSON'
            self.update_stat('!INFO', 'NO_LAST_OR_BUS_NAME', input_row_num)

        #--mandatory attributes
        json_data['DATA_SOURCE'] = 'LEIE'

        #--record type is not mandatory, but should be PERSON or ORGANIATION
        json_data['RECORD_TYPE'] = record_type

        #--the record_id should be unique, remove this mapping if there is not one 
        json_data['RECORD_ID'] = record_id

        #--column mappings

        if record_type == 'PERSON':
            # columnName: LASTNAME
            # 95.73 populated, 40.27 unique
            #      SMITH (618)
            #      JOHNSON (526)
            #      WILLIAMS (456)
            #      BROWN (432)
            #      JONES (426)
            json_data['PRIMARY_NAME_LAST'] = raw_data['LASTNAME']

            # columnName: FIRSTNAME
            # 95.74 populated, 16.17 unique
            #      MICHAEL (903)
            #      JOHN (882)
            #      ROBERT (849)
            #      JAMES (845)
            #      DAVID (750)
            json_data['PRIMARY_NAME_FIRST'] = raw_data['FIRSTNAME']

            # columnName: MIDNAME
            # 70.6 populated, 15.96 unique
            #      A (2583)
            #      L (2413)
            #      M (2296)
            #      J (1701)
            #      ANN (1681)
            json_data['PRIMARY_NAME_MIDDLE'] = raw_data['MIDNAME']

        # columnName: BUSNAME
        # 4.26 populated, 98.21 unique
        #      DUNN MEDICAL, INC (8)
        #      LEHIGH VALLEY COMMUNITY MENTAL (5)
        #      DENTAL HEALTH CARE CLINICS INC (3)
        #      GOLDSTAR HEALTHCARE, INC (3)
        #      MEDICAL ARTS PHARMACY (3)
        if raw_data['BUSNAME']:
            if record_type == 'PERSON':
                json_data['EMPLOYER_NAME'] = raw_data['BUSNAME']
            else:
                json_data['PRIMARY_NAME_ORG'] = raw_data['BUSNAME']

        # columnName: GENERAL
        # 100.0 populated, 0.12 unique
        #      IND- LIC HC SERV PRO (16047)
        #      NURSING PROFESSION (13013)
        #      MEDICAL PRACTICE, MD (4568)
        #      SKILLED NURSING FAC (4315)
        #      INDIVIDUAL (UNAFFILI (3569)
        json_data['GENERAL'] = raw_data['GENERAL']

        # columnName: SPECIALTY
        # 94.43 populated, 0.28 unique
        #      NURSE/NURSES AIDE (32393)
        #      OWNER/OPERATOR (3280)
        #      HEALTH CARE AIDE (2970)
        #      PERSONAL CARE PROVID (2096)
        #      NO KNOWN AFFILIATION (2045)
        json_data['SPECIALTY'] = raw_data['SPECIALTY']

        # columnName: UPIN
        # 8.34 populated, 97.11 unique
        #      A77906 (3)
        #      A73915 (3)
        #      D63434 (3)
        #      T55450 (3)
        #      B91173 (3)
        json_data['UPIN_NUMBER'] = raw_data['UPIN']

        # columnName: NPI
        # 100.0 populated, 7.7 unique
        #      0000000000 (68722)
        #      1801839139 (3)
        #      1225072028 (3)
        #      1215968847 (2)
        #      1154579191 (2)
        if raw_data['NPI'] and raw_data['NPI'] != '0000000000':
            json_data['NPI_NUMBER'] = raw_data['NPI']

        # columnName: DOB
        # 94.58 populated, 29.52 unique
        #      19670123 (18)
        #      19780927 (15)
        #      19690828 (15)
        #      19620116 (15)
        #      19621104 (15)
        if raw_data['DOB']:
            if record_type == 'PERSON':
                json_data['DATE_OF_BIRTH'] = self.format_date(raw_data['DOB'])
            else:
                self.update_stat('!INFO', 'BUS_NAME_HAS_DOB', input_row_num)

        # columnName: ADDRESS
        # 99.99 populated, 94.72 unique
        #      P O BOX 019120 (95)
        #      P O BOX 1032 (67)
        #      P O BOX 2000 (41)
        #      P O BOX 779800 (38)
        #      P O BOX 1000 (36)
        json_data['PRIMARY_ADDR_LINE1'] = raw_data['ADDRESS']

        # columnName: CITY
        # 100.0 populated, 13.23 unique
        #      MIAMI (1923)
        #      LOS ANGELES (629)
        #      PHOENIX (587)
        #      HOUSTON (556)
        #      BROOKLYN (542)
        json_data['PRIMARY_ADDR_CITY'] = raw_data['CITY']

        # columnName: STATE
        # 99.99 populated, 0.08 unique
        #      CA (9132)
        #      FL (7699)
        #      TX (5177)
        #      NY (4261)
        #      OH (3148)
        json_data['PRIMARY_ADDR_STATE'] = raw_data['STATE']

        # columnName: ZIP
        # 100.0 populated, 23.17 unique
        #      33101 (404)
        #      33521 (350)
        #      33177 (246)
        #      24910 (220)
        #      76127 (155)
        json_data['PRIMARY_ADDR_POSTAL_CODE'] = raw_data['ZIP']

        # columnName: EXCLTYPE
        # 100.0 populated, 0.03 unique
        #      1128b4 (30940)
        #      1128a1 (22200)
        #      1128a2 (7141)
        #      1128a3 (4549)
        #      1128a4 (2978)
        json_data['EXCLUSION_TYPE'] = raw_data['EXCLTYPE']

        # columnName: EXCLDATE
        # 100.0 populated, 3.08 unique
        #      20140520 (568)
        #      20091220 (497)
        #      20170720 (472)
        #      20100120 (469)
        #      20150820 (467)
        json_data['EXCLUSION_DATE'] = self.format_date(raw_data['EXCLDATE'])

        # columnName: REINDATE
        # 100.0 populated, 0.0 unique
        #      00000000 (74584)
        if raw_data['REINDATE'] and raw_data['REINDATE'] != '00000000':  #--maybe it will have a real value someday
            json_data['REINSTATED_DATE'] = self.format_date(raw_data['REINDATE'])

        # columnName: WAIVERDATE
        # 100.0 populated, 0.02 unique
        #      00000000 (74571)
        #      20090618 (1)
        #      20140429 (1)
        #      20160218 (1)
        #      20100820 (1)
        if raw_data['WAIVERDATE'] and raw_data['WAIVERDATE'] != '00000000':
            json_data['WAIVER_DATE'] = self.format_date(raw_data['WAIVERDATE'])

        # columnName: WVRSTATE
        # 0.02 populated, 75.0 unique
        #      NM (2)
        #      AR (2)
        #      MT (2)
        #      MP (1)
        #      GA (1)
        json_data['WAIVER_STATE'] = raw_data['WVRSTATE']

        #--remove empty attributes and capture the stats
        json_data = self.remove_empty_tags(json_data)
        self.capture_mapped_stats(json_data)

        return json_data

    #----------------------------------------
    def load_reference_data(self):

        #--garabage values
        self.variant_data = {}
        self.variant_data['GARBAGE_VALUES'] = ['NULL', 'NUL', 'N/A']

    #-----------------------------------
    def clean_value(self, raw_value):
        if not raw_value:
            return ''
        new_value = ' '.join(str(raw_value).strip().split())
        if new_value.upper() in self.variant_data['GARBAGE_VALUES']: 
            return ''
        return new_value

    #-----------------------------------
    def compute_record_hash(self, target_dict, attr_list = None):
        if attr_list:
            string_to_hash = ''
            for attr_name in sorted(attr_list):
                string_to_hash += (' '.join(str(target_dict[attr_name]).split()).upper() if attr_name in target_dict and target_dict[attr_name] else '') + '|'
        else:           
            string_to_hash = json.dumps(target_dict, sort_keys=True)
        return hashlib.md5(bytes(string_to_hash, 'utf-8')).hexdigest()

    #----------------------------------------
    def format_date(self, raw_date):
        try: 
            return datetime.strftime(dateparse(raw_date), '%Y-%m-%d')
        except: 
            self.update_stat('!INFO', 'BAD_DATE', raw_date)
            return ''

    #----------------------------------------
    def format_dob(self, raw_date):
        try: new_date = dateparse(raw_date)
        except: return ''

        #--correct for prior century dates
        if new_date.year > datetime.now().year:
            new_date = datetime(new_date.year - 100, new_date.month, new_date.day)

        if len(raw_date) == 4:
            output_format = '%Y'
        elif len(raw_date) in (5,6):
            output_format = '%m-%d'
        elif len(raw_date) in (7,8):
            output_format = '%Y-%m'
        else:
            output_format = '%Y-%m-%d'

        return datetime.strftime(new_date, output_format)

    #----------------------------------------
    def remove_empty_tags(self, d):
        if isinstance(d, dict):
            for  k, v in list(d.items()):
                if v is None or len(str(v).strip()) == 0:
                    del d[k]
                else:
                    self.remove_empty_tags(v)
        if isinstance(d, list):
            for v in d:
                self.remove_empty_tags(v)
        return d

    #----------------------------------------
    def update_stat(self, cat1, cat2, example=None):

        if cat1 not in self.stat_pack:
            self.stat_pack[cat1] = {}
        if cat2 not in self.stat_pack[cat1]:
            self.stat_pack[cat1][cat2] = {}
            self.stat_pack[cat1][cat2]['count'] = 0

        self.stat_pack[cat1][cat2]['count'] += 1
        if example:
            if 'examples' not in self.stat_pack[cat1][cat2]:
                self.stat_pack[cat1][cat2]['examples'] = []
            if example not in self.stat_pack[cat1][cat2]['examples']:
                if len(self.stat_pack[cat1][cat2]['examples']) < 5:
                    self.stat_pack[cat1][cat2]['examples'].append(example)
                else:
                    randomSampleI = random.randint(2, 4)
                    self.stat_pack[cat1][cat2]['examples'][randomSampleI] = example
        return

    #----------------------------------------
    def capture_mapped_stats(self, json_data):

        if 'DATA_SOURCE' in json_data:
            data_source = json_data['DATA_SOURCE']
        else:
            data_source = 'UNKNOWN_DSRC'

        for key1 in json_data:
            if type(json_data[key1]) != list:
                self.update_stat(data_source, key1, json_data[key1])
            else:
                for subrecord in json_data[key1]:
                    for key2 in subrecord:
                        self.update_stat(data_source, key2, subrecord[key2])

#----------------------------------------
def signal_handler(signal, frame):
    print('USER INTERUPT! Shutting down ... (please wait)')
    global shut_down
    shut_down = True
    return

#----------------------------------------
if __name__ == "__main__":
    proc_start_time = time.time()
    shut_down = False   
    signal.signal(signal.SIGINT, signal_handler)

    input_file = 'input/leie-full-2021-08-20.csv'
    csv_dialect = 'excel'

    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input_file', dest='input_file', default = input_file, help='the name of the input file')
    parser.add_argument('-o', '--output_file', dest='output_file', help='the name of the output file')
    parser.add_argument('-l', '--log_file', dest='log_file', help='optional name of the statistics log file')
    args = parser.parse_args()

    if not args.input_file or not os.path.exists(args.input_file):
        print('\nPlease supply a valid input file name on the command line\n')
        sys.exit(1)
    if not args.output_file:
        print('\nPlease supply a valid output file name on the command line\n') 
        sys.exit(1)

    input_file_handle = open(args.input_file, 'r')
    output_file_handle = open(args.output_file, 'w', encoding='utf-8')
    mapper = mapper()

    input_row_count = 0
    output_row_count = 0
    for input_row in csv.DictReader(input_file_handle, dialect=csv_dialect):
        input_row_count += 1

        json_data = mapper.map(input_row, input_row_count)
        if json_data:
            output_file_handle.write(json.dumps(json_data) + '\n')
            output_row_count += 1

        if input_row_count % 1000 == 0:
            print('%s rows processed, %s rows written' % (input_row_count, output_row_count))
        if shut_down:
            break

    elapsed_mins = round((time.time() - proc_start_time) / 60, 1)
    run_status = ('completed in' if not shut_down else 'aborted after') + ' %s minutes' % elapsed_mins
    print('%s rows processed, %s rows written, %s\n' % (input_row_count, output_row_count, run_status))

    output_file_handle.close()
    input_file_handle.close()

    #--write statistics file
    if args.log_file: 
        with open(args.log_file, 'w') as outfile:
            json.dump(mapper.stat_pack, outfile, indent=4, sort_keys = True)
        print('Mapping stats written to %s\n' % args.log_file)


    sys.exit(0)

