"""
   Copyright (C) 2017 STFC.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""


import logging

from apel.db.records.dataset import DataSetRecord
from apel.db.records.group_attribute import GroupAttributeRecord
from apel.common.datetime_utils import parse_timestamp
from xml_parser import XMLParser, XMLParserException


log = logging.getLogger(__name__)


class DSarParser(XMLParser):
    """
    Parser for DataSet Accounting Records.

    For documentation please visit:
    # TODO: some link
    """

    # TODO: Add actual link?
    NAMESPACE = 'http://eu-emi.eu/namespaces/2017/01/datasetrecord'  # None

    def get_records(self):
        """
        Return a list of parsed records from DSAR file.

        Please notice that this parser _requires_ valid
        structure of XML document, including namespace
        information and prefixes in XML tag (like ur:DataSetUsageRecord).
        """
        returned_records = []

        # Extract the individual usage records as a list
        xml_dataset_records = self.doc.getElementsByTagNameNS(
            self.NAMESPACE, 'UsageRecord')

        for record in xml_dataset_records:
 
            data = {}

            # Extract the RecordIdentityBlock
            xml_record_identity_block = record.getElementsByTagNameNS(
                self.NAMESPACE, 'RecordIdentityBlock')

            # Check there is only one
            if len(xml_record_identity_block) is not 1:
                raise XMLParserException("Exactly one RecordIdentityBlock required.")

            # Parse it
            data = self.parse_record_identity_block(xml_record_identity_block[0], data)

            # Extract the SubjectIdentityBlock
            xml_subject_identity_block = record.getElementsByTagNameNS(
                self.NAMESPACE, 'SubjectIdentityBlock')

            # Check there is only one
            if len(xml_subject_identity_block) is not 1:
                raise XMLParserException("Exactly one SubjectIdentityBlock required.")

            # Parse it
            data = self.parse_subject_identity_block(xml_subject_identity_block[0], data)

            # Extract the DataSetUsageBlcok
            xml_dataset_usage_block = record.getElementsByTagNameNS(
                self.NAMESPACE, 'DataSetUsageBlock')

            # Check there is only one
            if len(xml_dataset_usage_block) is not 1:
                raise XMLParserException("Exactly one DataSetUsageBlock required.")

            # Parse it
            data = self.parse_dataset_usage_block(xml_dataset_usage_block[0], data)

            group_attributes = self.parse_group_attributes(
                record.getElementsByTagNameNS(
                    self.NAMESPACE, 'GlobalGroupAttribute'), data['RecordId'])

            record = DataSetRecord()
            record.set_all(data)

            returned_records.append(record)
            returned_records += group_attributes
        return returned_records

    def parse_record_identity_block(self, xml_record_identity_block, data={}):
        """
        Parse a single RecordIdentityBlock into a dictionary.

        If provided, the parsed information will be stored in data.
        """
        functions = {

            'RecordId': lambda nodes: self.getText(
                nodes['RecordId'][0].childNodes).strip("\""),
            'CreateTime': lambda nodes: self.getText(
                nodes['CreateTime'][0].childNodes),
            'ResourceProvider': lambda nodes: self.getText(
                nodes['ResourceProvider'][0].childNodes).strip("\""),
        }

        # Here we copy keys from functions.
        nodes = {}.fromkeys(functions.keys())

        for node in nodes:
            nodes[node] = xml_record_identity_block.getElementsByTagNameNS(
                self.NAMESPACE, node)

        for field in functions:
            try:
                data[field] = functions[field](nodes)
            except (IndexError, KeyError), e:
                log.debug("Failed to get field %s: %s", field, e)

        return data

    def parse_dataset_usage_block(self, xml_dataset_record, data={}):
        """
        Parse a single DataSetUsageBlock.

        If provided, the parsed information will be stored in data.
        """
        functions = {
            'DataSetID': lambda nodes: self.getText(
                nodes['DataSetID'][0].childNodes).strip("\""),
            'DataSetIDType': lambda nodes: self.getText(
                nodes['DataSetIDType'][0].childNodes).strip("\""),
            'ReadAccessEvents': lambda nodes: self.getText(
                nodes['ReadAccessEvents'][0].childNodes),
            'WriteAccessEvents': lambda nodes: self.getText(
                nodes['WriteAccessEvents'][0].childNodes),
            'Source': lambda nodes: self.getText(
                nodes['Source'][0].childNodes).strip("\""),
            'Destination': lambda nodes: self.getText(
                nodes['Destination'][0].childNodes).strip("\""),
            'StartTime': lambda nodes: self.getText(
                nodes['StartTime'][0].childNodes),
            'Duration': lambda nodes: self.getText(
                nodes['Duration'][0].childNodes),
            'EndTime': lambda nodes: self.getText(
                nodes['EndTime'][0].childNodes),
            'TransferSize': lambda nodes: self.getText(
                nodes['TransferSize'][0].childNodes),
            'HostType': lambda nodes: self.getText(
                nodes['HostType'][0].childNodes).strip("\""),
            'FileCount': lambda nodes: self.getText(
                nodes['FileCount'][0].childNodes),
            'Status': lambda nodes: self.getText(
                nodes['Status'][0].childNodes).strip("\""),
            }

        # Here we copy keys from functions.
        nodes = {}.fromkeys(functions.keys())

        for node in nodes:
            nodes[node] = xml_dataset_record.getElementsByTagNameNS(
                self.NAMESPACE, node)
            # empty list = element have not been found in XML file

        for field in functions:
            try:
                data[field] = functions[field](nodes)
            except (IndexError, KeyError), e:
                log.debug("Failed to get field %s: %s", field, e)

        return data

    def parse_subject_identity_block(self, xml_subject_identity_block, data={}):
        """
        Parse a single SubjectIdentityBlock.

        If provided, the parsed information will be stored in data.
        """
        functions = {
            'GlobalUserId': lambda nodes: self.getText(
                nodes['GlobalUserId'][0].childNodes).strip("\""),
            'GlobalGroupId': lambda nodes: self.getText(
                nodes['GlobalGroupId'][0].childNodes).strip("\""),
            'ORCHID': lambda nodes: self.getText(
                nodes['ORCHID'][0].childNodes).strip("\""),
        }

        # Here we copy keys from functions.
        nodes = {}.fromkeys(functions.keys())

        for node in nodes:
            nodes[node] = xml_subject_identity_block.getElementsByTagNameNS(
                self.NAMESPACE, node)
            # empty list = element have not been found in XML file

        for field in functions:
            try:
                data[field] = functions[field](nodes)
            except (IndexError, KeyError), e:
                log.debug("Failed to get field %s: %s", field, e)

        return data

    def parse_group_attributes(self, nodes, record_id):
        """Return a list of GroupAttributeRecords associated with a DSarRecord."""
        ret = []

        for node in nodes:
            group_attr = GroupAttributeRecord()
            group_attr.set_field('RecordId', record_id)
            attr_type = self.getAttr(node, 'type')#node.getAttribute('ur:type')
            group_attr.set_field('AttributeType', attr_type)
            attr_value = self.getText(node.childNodes)
            group_attr.set_field('AttributeValue', attr_value)
            ret.append(group_attr)

        return ret
