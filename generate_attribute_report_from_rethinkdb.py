#! /usr/bin/python

# Generate_attribute_report_from_Rethinkdb is exactly what it sounds like, it will use grapper to upload
# data parsed from the RethinkDB to Google Sheets.

import rethinkdb as rdb
import re
from simech_common import *
import grapper
import argparse
import csv
from copy import copy
import os


version = ".1"

parser = argparse.ArgumentParser(conflict_handler="resolve",
usage="""generate_attribute_report_from_rethinkdb
Generate a report of the SMART attributes on drives we've encountered
at Silicon Mechanics.

""", version = version)

parser.add_argument("-n", "--name", action="store", dest="file", default="",
help="The spreadsheet name to upload to")

# options now contains the flags we'll check.
options = parser.parse_args()

# Set up Rethinkdb
rip = "10.101.24.21"
rport = "28015"

# Little helper for wonky customer names
def filter_bs_chars(stir):
    if not stir:
        return stir
    # End if we weren't sent anything

    stir = re.sub("\\|/|~|\"", "", stir)
    stir = re.sub(" ", "_", stir)
    return stir
# End filter_bs_chars


class Drive():
    def __init__(self):
        self.sm_number = ""
        self.project_number = ""
        self.order_number = ""
        self.customer_name = ""

        self.serial = ""
        self.model = ""
        self.manufacturer = ""

        self.attribute_table = {}

        # Number of entries in RethinkDB for this serial
        self.number_of_entries = 1

        # Mash up an IEEE Date stamp in to a giant integer
        self.timestamp = 0

    def show(self):
        for k, v in vars(self):
            print color(k, ylw) + ":  " + str(v)
        print

# End Drive()

def build_drive_data_hash(max = 0):
    """ Build the master list of drives to write data out from

    params: max - number of entries to go through; default all
    """

    rdb.connect(rip, rport, db="production").repl()
    # Get a cursor object from rethinkdb
    cursor = rdb.table('logs').run()
    count = 0
    drives_hash = {}
    try:
        while cursor.items and count <= max:
            data_hash = get_drive_data_from_thash(cursor.items[0])
            for k, v in data_hash.items():
                if k in drives_hash:
                    if drives_hash[k][0] < data_hash[k][0]:
                        drives_hash[k][1] = copy(data_hash[k][1])
                        # Incrememnt the number of entries this serial number
                        # has since this will be the second time we've seen it
                        drives_hash[k][1].number_of_entries += 1
                else:
                    drives_hash[k] = copy(data_hash[k])
            # End loop through returned data hash
            cursor.next()
            if not cursor.items:
                cursor.next()

            if max:
                count += 1
        # End while we're less than count
    except KeyboardInterrupt:
        pass
    except Exception as e:
        error("Uh error {} occurred but we're gonna just return the list now".format(str(e)))

    return drives_hash
# End build_drive_data_hash

def get_drive_data_from_thash(thash):
    """get_drive_data_from_thash goes through the dictionary given to it and builds a list of
    Drive objects.

    It returns a hash with keys of drives serials and value of a list where the first entry
    is a timestamp and the second is the Drive object.

    params: thash
    """
    for key in ["SM Number", "Project Number", "Order Number", "Customer Name", "Components", "IEEE Datetime"]:
        if key not in thash:
            error("Couldn't find {} in thash?  Keys were:".format(key))
            print thash.keys()
            return False
    # End check for critical key names

    return_hash = {}

    for dhash in thash['Components']['Drives']:
        d = Drive()
        for key in ["SM Number", "Project Number", "Order Number", "Customer Name"]:
            setattr(d, re.sub(" ", "_", key.lower()), thash[key])

        d.serial = dhash['Serial']
        d.model = dhash['Model']
        d.manufacturer = dhash['Manufacturer']
        d.timestamp = int(re.sub("-", "", thash['IEEE Datetime']))
        try:
            d.attribute_table = dhash['Smartctl Attribute Table']
        except Exception:
            try:
                d.attribute_table = dhash['Smartctl attribute table']
            except Exception as e:
                error("Couldn't find smartctl attribute table in {} {}? Keys are:".format(d.model, d.serial))
                print dhash.keys()
                  
        if d.serial and d.attribute_table:
            return_hash[d.serial] = [d.timestamp, d]

    return return_hash
# End get_Drive_data_from_thash()

def get_attribute_value(drv, attribute):
    if not drv or not attribute:
        return None

    for k, v in drv.attribute_table.items():
        if re.search("^" + str(attribute) + " ", k):
            # If we found our attribute, return the last entry in the value list
            # filtered through the regex below
            return int(search("([0-9]+)", v[-1], max_matches = 1))

    return None
#End get_attribute_value

def show_drive_data(dlist):
    # Count of drives with UDMA attribute
    c = 0
    c_greater = 0
    for d in dlist:
        #print color("Model / Serial:  {}, {}".format(d.model, d.serial), ylw)
        #print "  Timestamp: {}".format(d.timestamp)
        amt = get_attribute_value(d, "199")
        if amt or amt == 0:
            c += 1
            if amt > 0:
                c_greater += 1

    print color("Of {} disks, {} had an attribute 199.  That's {:.1f}%.".format(len(dlist), 
        c, float(c)/float(len(dlist)) * 100), grn)
    print color("Of {} disks with attribute 199, {} had more than 0.  That's {:.1f}%.".format(c, 
        c_greater, float(c_greater)/float(c) * 100), grn)
# End show_drive_data

def upload(dlist):
    if not dlist:
        return False

    data = ["Time Stamp", "Customer", "Order", "Project", "SM Number", "Log Link", "Manufacturer", "Model", "Serial", "UDMA Errors", "Number of Entries"]
    grapper.replace_worksheet("UDMA Errors - All SATA Drives", "Data", data)
    grapper.replace_worksheet("UDMA Errors - Just Drives with Attribute 199", "Data", data)
    count = 0
    rawlist = []
    for d in dlist:
        data = {}
        data['Time Stamp'] = d.timestamp
        data['Customer'] = d.customer_name
        data['Order'] = d.order_number
        data['Project'] = d.project_number
        data['SM Number'] = d.sm_number
        data['Log Link'] = "http://jarvis/production_automation/burn_logs/" + \
            d.project_number + "_" + filter_bs_chars(d.customer_name) + "/"
        data['Manufacturer'] = d.manufacturer
        data['Model'] = d.model
        data['Serial'] = d.serial
        att = get_attribute_value(d, "199")
        if att == None:
            att = "N/A"
        data['UDMA Errors'] = att
        data['Number of Entries'] = d.number_of_entries
        rawlist.append(data)
    # End loop through drive list

    # Now make some sorted lists here because Google sheets can't handle managing
    # something that is 65,000 lines long
    only_udma_drives = [x for x in rawlist if not x['UDMA Errors'] == "N/A"]
    only_udma_drives.sort(key=lambda x: int(x['UDMA Errors']), reverse=True)
    raw_sorted_by_udma = sorted(rawlist, key=lambda x: x['UDMA Errors'], reverse=True)

    for lst, name in ([raw_sorted_by_udma, "UDMA Errors - All SATA Drives"], [only_udma_drives, "UDMA Errors - Just Drives with Attribute 199"]):
        uplist = []
        count = 0
        for item in lst:
            uplist.append(item)
            count += 1
            if count == 5000:
                print "Appending {} entries to {}....".format(len(uplist), name)
                grapper.append_worksheet(name, "Data", uplist)
                count = 0
                uplist = []
        
        if not count == 0:
            print "Appending {} entries to {}...".format(len(uplist), name)
            grapper.append_worksheet(name, "Data", uplist)

        grapper.make_pretty(name, "Data")
    # End loop through our two lists

# End upload()

dhash = build_drive_data_hash()
dlist = [v[1] for k, v in dhash.items()]
show_drive_data(dlist)
upload(dlist)

























