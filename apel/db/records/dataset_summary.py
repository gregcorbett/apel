"""
   Copyright 2017 STFC.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

@author Will Rogers, Greg Corbett
"""

import logging
import time

from datetime import datetime
from xml.dom.minidom import Document

from apel.db.records import Record, InvalidRecordException

# get the relevant logger
logger = logging.getLogger(__name__)


class DataSetSummaryRecord(Record):
    """
    Class to represent one dataset summary record.

    It knows about the structure of the MySQL table and the message format.
    It stores its information in a dictionary self._record_content.  The keys
    are in the same format as in the messages, and are case-sensitive.
    """

    MANDATORY_FIELDS = []

    # This list specifies the information that goes in the database.
    DB_FIELDS = ["ResourceProvider", "Infrastructure", "GlobalUserId",
                 "GlobalGroupId", "ORCID", "DataSetID", "DataSetIDType",
                 "TotalReadAccessEvents", "TotalWriteAccessEvents",
                 "Source", "Destination", "EarliestStartTime", "TotalDuration",
                 "LatestStartTime", "Month", "Year", "TotalTransferSize",
                 "HostType", "TotalFileCount", "Status"]

    ALL_FIELDS = DB_FIELDS

    def __init__(self):
        """Provide the necessary lists containing message information."""
        Record.__init__(self)

        # Fields which are required by the message format.
        self._mandatory_fields = DataSetSummaryRecord.MANDATORY_FIELDS

        # This list specifies the information that goes in the database.
        self._db_fields = DataSetSummaryRecord.DB_FIELDS

        # Fields which are accepted but currently ignored.
        self._ignored_fields = ["UpdateTime"]

        self._all_fields = self._db_fields

        # Fields which will have a date time stored in them
        self._datetime_fields = ["EarliestStartTime", "LatestStartTime"]

        # Fields which will have an integer stored in them
        self._int_fields = ["TotalReadAccessEvents", "TotalWriteAccessEvents",
                            "Month", "Year", "TotalTransferSize",
                            "TotalFileCount"]

    def get_ur(self, withhold_dns=False):
        """
        Returns the DataSetSummaryRecord in DSAR format.
        """
        doc = Document()
        ur = doc.createElement('ur:UsageSummaryRecord')

        create_time = self.get_field('CreateTime')
        cr_time = doc.createElement('ur:CreateTime')
        cr_time.appendChild(doc.createTextNode(datetime.now().strftime('%Y-%m-%dT%H:%M:%S')))
        ur.appendChild(cr_time)       

        for field in self._all_fields:
           text = self.get_field(field)
           if text is None:
               if field in self._mandatory_fields:
                   raise InvalidRecordException('%s is mandatory but not in Database entry.' % field)
               else:
                   logger.debug('%s is None, skipping.' % field)
                   continue

           elem = doc.createElement('ur:%s' % field)
           elem.appendChild(doc.createTextNode(str(text)))
           ur.appendChild(elem)

        doc.appendChild(ur)
        return doc.documentElement.toxml()

#        rec_id.setAttribute('us:CreateTime', datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
#        rec_id.setAttribute('us:RecordId', record_id)
        
        #resource_provider = self.get_field('ResourceProvider')
        #res_provider = doc.createElement('us:ResourceProvider')
        #res_provider.appendChild(doc.createTextNode(resource_provider))
        #us.appendChild(res_provider)
        

#    def get_apel_db_insert(self, apel_db, source):
#        """
#        Return record content as a tuple, appending the source of the record.

#        (i.e. the sender's DN).  Also returns the appropriate stored procedure.

#        We have to go back to the apel_db object to find the stored procedure.
#        This is because only this object knows what type of record it is,
#        and only the apel_db knows what the procedure details are.
#        """
        # TODO: apel_db is unused here?!
#        values = self.get_db_tuple(self, source)

#        return values

#    def get_db_tuple(self, source=None):
#        """
#        Return record contents as tuple ignoring the 'source' keyword argument.
#
#        The source (DN of the sender) isn't used in this record type currently.
#        """
#        return Record.get_db_tuple(self)
