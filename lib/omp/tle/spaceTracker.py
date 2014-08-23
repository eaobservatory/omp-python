"""Builds and checks ids, builds api request,
   sends request and returns list of tles
   """

import omp.siteconfig as siteconfig
from requests import session


class SpaceTrack(object):
    """space-track.org API"""
    def __init__(self, tletype="NORAD"):
        self.tletype = tletype
        self.id_list = []
        self.req_list = []
        self.rurl = ""

    def add_id(self, catid):
        """Take NORAD Cat ID and adds it to the list."""
        if type(catid) is not str:
            catid = str(catid)
        if len(catid) > 5:
            try:
                raise ValueError("NORAD catid requires 5 or fewer digits.")
            except ValueError:
                print "Not good."
                raise
            return
        self.id_list.append(catid)

    def build_request(self):
        """Builds a request"""
        #Turns all ids into integers to strip spurious preceding 0's
        temp_set = set([int(r) for r in self.id_list])
        #Sort back into a list.
        temp_list = sorted(list(temp_set))
        temp_list = [str(r) for r in temp_list]
        #comma separates
        temp_str = ",".join(temp_list)
        #Currently just what we need. But this could be parameterized.
        self.rurl = ("https://www.space-track.org/basicspacedata/query/class/tle_latest/ORDINAL/1/NORAD_CAT_ID/" +
                     temp_str + "/orderby/EPOCH desc/format/tle")

    def send_request(self):
        """Sends current request."""
        url = "https://www.space-track.org/ajaxauth/login"
        cfg = siteconfig.get_omp_siteconfig()
        user = cfg.get('spacetrack', 'user')
        password = cfg.get('spacetrack', 'password')
        idpass = {'identity': user, 'password': password}
        r = None
        with session() as ss:
            r = ss.post(url, data=idpass)
            r = ss.get(self.rurl)
        return r.text
