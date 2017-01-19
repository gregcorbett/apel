import datetime
import unittest

from apel.db.loader import DSarParser
from apel.db.backends.mysql import ApelMysqlDb


class DSarParserTest(unittest.TestCase):
    """Test case for DSar Parser."""

    def test_star_parser(self):
        """Test parsing the test message."""
        parser = DSarParser(self.MESSAGE_1)
        records = parser.get_records()

        # Assert parsed records is the expected length
        self.assertEqual(len(records), len(self.RESULT_1))

        # Check that each individual
        # DataSet/GroupAttribute is correct
        for i in range(0, len(records)):
            self.assertEqual(records[i].get_db_tuple(), self.RESULT_1[i])

    MESSAGE_1 = """<?xml version="1.0" encoding="UTF-8"?>
    <ur:UsageRecords xmlns:ur="http://eu-emi.eu/namespaces/2017/01/datasetrecord">
        <ur:UsageRecord>
            <ur:RecordIdentityBlock>
                <ur:RecordId>"host.example.org/ur/2</ur:RecordId>
                <ur:CreateTime>2017-01-17T18:06:52</ur:CreateTime>
                <ur:ResourceProvider>"EGI"</ur:ResourceProvider>
            </ur:RecordIdentityBlock>
            <ur:SubjectIdentityBlock>
                <ur:GlobalUserId>"user1"</ur:GlobalUserId>
                <ur:GlobalGroupId>"VO1"</ur:GlobalGroupId>
                <ur:GlobalGroupAttribute ur:type="subgroup">ukusers</ur:GlobalGroupAttribute>
                <ur:GlobalGroupAttribute ur:type="role">super</ur:GlobalGroupAttribute>
                <ur:GlobalGroupAttribute ur:type="authority">UK</ur:GlobalGroupAttribute>
                <ur:ORCHID>"123456789"</ur:ORCHID>
            </ur:SubjectIdentityBlock>
            <ur:DataSetUsageBlock>
                <ur:DataSet>"dataset-1"</ur:DataSet>
                <ur:AccessEvents>5</ur:AccessEvents>
                <ur:Source>"vm18"</ur:Source>
                <ur:Destination>"vm45"</ur:Destination>
                <ur:StartTime>2017-01-17T09:28:00</ur:StartTime>
                <ur:Duration>30000</ur:Duration>
                <ur:EndTime>2017-01-17T17:48:00</ur:EndTime>
                <ur:TransferSize>9001</ur:TransferSize>
                <ur:HostType>"Disk"</ur:HostType>
                <ur:FileCount>4</ur:FileCount>
                <ur:Status>"Success"</ur:Status>
            </ur:DataSetUsageBlock>
        </ur:UsageRecord>
        <ur:UsageRecord>
            <ur:RecordIdentityBlock>
                <ur:RecordId>"host2.example.org/ur/1</ur:RecordId>
                <ur:CreateTime>2016-01-17T18:06:52</ur:CreateTime>
                <ur:ResourceProvider>"EGI"</ur:ResourceProvider>
            </ur:RecordIdentityBlock>
            <ur:SubjectIdentityBlock>
                <ur:GlobalUserId>"user2"</ur:GlobalUserId>
                <ur:GlobalGroupId>"VO2"</ur:GlobalGroupId>
                <ur:ORCHID>"987654321"</ur:ORCHID>
            </ur:SubjectIdentityBlock>
            <ur:DataSetUsageBlock>
                <ur:DataSet>"dataset-2"</ur:DataSet>
                <ur:AccessEvents>5</ur:AccessEvents>
                <ur:Source>"vm18"</ur:Source>
                <ur:Destination>"vm45"</ur:Destination>
                <ur:StartTime>2016-01-17T09:28:00</ur:StartTime>
                <ur:Duration>30000</ur:Duration>
                <ur:EndTime>2016-01-17T17:48:00</ur:EndTime>
                <ur:TransferSize>9001</ur:TransferSize>
                <ur:HostType>"Disk"</ur:HostType>
                <ur:FileCount>5</ur:FileCount>
                <ur:Status>"Success"</ur:Status>
            </ur:DataSetUsageBlock>
        </ur:UsageRecord>
    </ur:UsageRecords>"""

    RESULT_1A = ('host.example.org/ur/2',
                 datetime.datetime(2017, 1, 17, 18, 6, 52), 'EGI', 'user1',
                 'VO1', '123456789', 'dataset-1', 5, 'vm18', 'vm45',
                 datetime.datetime(2017, 1, 17, 9, 28), '30000',
                 datetime.datetime(2017, 1, 17, 17, 48), 9001,
                 'Disk', 4, 'Success')

    RESULT_1B = ('host.example.org/ur/2', 'subgroup', 'ukusers')

    RESULT_1C = ('host.example.org/ur/2', 'role', 'super')

    RESULT_1D = ('host.example.org/ur/2', 'authority', 'UK')

    RESULT_1E = ('host2.example.org/ur/1',
                 datetime.datetime(2016, 1, 17, 18, 6, 52), 'EGI', 'user2',
                 'VO2', '987654321', 'dataset-2', 5, 'vm18', 'vm45',
                 datetime.datetime(2016, 1, 17, 9, 28), '30000',
                 datetime.datetime(2016, 1, 17, 17, 48), 9001,
                 'Disk', 5, 'Success')

    RESULT_1 = [RESULT_1A, RESULT_1B, RESULT_1C, RESULT_1D, RESULT_1E]

if __name__ == '__main__':
    unittest.main()
