# from requests import session
# import sys

# def helpUser():
#     '''Prints help directions'''
#     print '''spaceTracker.py -- For use by JAC
# ********************************************
# Please enter a list of space separated satellite numbers or
# a path to a text file.'''
#     return

# def parse(nums_in): #not going to work. Will take one satellite the wrong way.
#     list_nums = []
#     if type(nums_in) is list:
#         #set list to list
#         list_nums = nums_in
#     if type(nums_in) is str:
#         #open file and get list into list_nums
#         pass
#     #check list_nums to see that they conform to the NORAD_CAT_ID

# #if len(sys.argv) < 2:
# #    helpUser()
# #    quit()

# url = "https://www.space-track.org/ajaxauth/login"
# user = "ukirtot@jach.hawaii.edu"
# password = ""
# idpass = {'identity': user, 'password': password}

# with session() as ss:
#     r = ss.post(url, data=idpass)
#     print r.text
#     rurl = "https://www.space-track.org/basicspacedata/query/class/tle/NORAD_CAT_ID/25544/orderby/EPOCH desc/limit/22/format/tle"
#     r = ss.get(rurl)
#     print r.text

class SpaceTrack(object):
    """space-track.org API"""
    def __init__(self):
        self.id_list = []
        self.req_list = []

    def add_id(self, catid):
        """Take NORAD Cat ID and adds it to the list."""
        if len(catid) > 5:
            try:
                raise ValueError("catid requires 5 or fewer digits.")
            except ValueError:
                print "Not good."
                raise
            return
        elif len(catid) < 5:
            temp = (5 - len(catid)) * '0'
            catid = temp + catid
        self.id_list.append(catid)

    def build_requests(self):
        """Lumps the IDs that can be lumped and creates a batch of requests."""
        pass

    def send_requests(self):
        """Sends current batch of requests."""
        pass
