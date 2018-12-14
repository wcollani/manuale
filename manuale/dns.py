import logging
import time
import dns.resolver

import boto3

# TODO: "ImportError: No module named 'azure.common'" after setup.py install
from azure.common.client_factory import get_client_from_cli_profile
from azure.mgmt.dns import DnsManagementClient

from .errors import ManualeError

logger = logging.getLogger(__name__)


class DnsProvider(object):
    provider = "BaseDnsProvider"
    ttl = 60
    attempts = 5
    sleep_duration = 15

    def create_dns_record(self, domain, txt_record):
        logger.info("create_dns_record")
        raise ManualeError(NotImplementedError)

    def delete_dns_record(self, domain, txt_record):
        logger.info("delete_dns_record")
        raise ManualeError(NotImplementedError)

    def validate_dns_record(self, domain, txt_record):
        logger.info("")
        logger.info("Verifying challenge record.")

        for i in range(1, self.attempts+1):
            logger.info("Checking in {0} seconds. Attempt {1}/{2}".format(self.sleep_duration, i, self.attempts))
            time.sleep(self.sleep_duration)

            try:
                response = dns.resolver.query(qname='_acme-challenge' + '.' + domain, rdtype='TXT')

                for rdata in response:
                    logger.info("Found record: {0} IN TXT {1}".format(
                        rdata.to_text(), '_acme-challenge' + '.' + domain))

                    if rdata.to_text() == '"' + txt_record + '"':
                        logger.info("Record matches challenge.")
                        return True
                else:
                    logger.info("Challenge record not found")

            except dns.resolver.NXDOMAIN:
                logger.info("Challenge record not found")
                pass

        return False


class Route53(DnsProvider):
    def __init__(self, hosted_zone_id):
        self.provider = "route53"
        self.hosted_zone_id = hosted_zone_id
        self.route53_client = boto3.client('route53')
        # TODO: Throw error if hosted zone is not accessible

    def create_dns_record(self, domain, txt_record):
        batch = {'Changes': [
                    {
                        'Action': 'UPSERT',
                        'ResourceRecordSet': {
                            'Name': '_acme-challenge' + '.' + domain + '.',
                            'Type': 'TXT',
                            'TTL': self.ttl,
                            'ResourceRecords': [{'Value': '"' + txt_record + '"'}]
                        }
                    }
                ]}
        logger.info("")
        logger.info("Creating record {}".format(batch))
        self.route53_client.change_resource_record_sets(
            HostedZoneId=self.hosted_zone_id,
            ChangeBatch=batch)

    def delete_dns_record(self, domain, txt_record):
        batch = {'Changes': [
                    {
                        'Action': 'DELETE',
                        'ResourceRecordSet': {
                            'Name': '_acme-challenge' + '.' + domain + '.',
                            'Type': 'TXT',
                            'TTL': self.ttl,
                            'ResourceRecords': [{'Value': '"' + txt_record + '"'}]
                        }
                    }
                ]}
        logger.info("")
        logger.info("Deleting record {}".format(batch))
        self.route53_client.change_resource_record_sets(
            HostedZoneId=self.hosted_zone_id,
            ChangeBatch=batch)


# TODO: Implement Azure
class Azure(DnsProvider):
    def __init__(self, resource_group):
        self.provider = "azure"
        self.attempts = 10
        self.azure_client = get_client_from_cli_profile(DnsManagementClient)
        self.resource_group = resource_group
        self.rg_domain = ""

    def create_dns_record(self, domain, txt_record):
        logger.info("")
        logger.info("Creating record.")

        for i in self.azure_client.zones.list_by_resource_group(resource_group_name=self.resource_group):
            if i.name in domain:
                self.rg_domain = i.name
                break

        if not self.rg_domain:
            raise ManualeError("Domain not found in resource group: {}".format(self.resource_group))

        self.azure_client.record_sets.create_or_update(
            resource_group_name=self.resource_group,
            zone_name=self.rg_domain,
            relative_record_set_name='_acme-challenge' + "." + domain.strip(self.rg_domain),
            record_type='TXT',
            parameters={
                'ttl': self.ttl,
                'txt_records': [{'value': [txt_record]}]
            }
        )

    def delete_dns_record(self, domain, txt_record):
        logger.info("")
        logger.info("Deleting record.")

        for i in self.azure_client.zones.list_by_resource_group(resource_group_name=self.resource_group):
            if i.name in domain:
                self.rg_domain = i.name
                break

        if not self.rg_domain:
            raise ManualeError("Domain not found in resource group: {}".format(self.resource_group))

        self.azure_client.record_sets.delete(
            resource_group_name=self.resource_group,
            zone_name=self.rg_domain,
            relative_record_set_name='_acme-challenge' + "." + domain.strip(self.rg_domain),
            record_type='TXT')
