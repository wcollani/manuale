import logging
import boto3
import time
import dns.resolver

from .errors import ManualeError

logger = logging.getLogger(__name__)


class DnsProvider(object):
    provider = "BaseDnsProvider"

    def create_dns_record(self, domain, txt_record):
        logger.info("create_dns_record")
        raise ManualeError(NotImplementedError)

    def delete_dns_record(self, domain, txt_record):
        logger.info("delete_dns_record")
        raise ManualeError(NotImplementedError)

    @staticmethod
    def validate_dns_record(domain, txt_record):
        attempts = 5
        sleep_duration = 15

        logger.info("")
        logger.info("Verifying challenge record.")

        for i in range(1, attempts+1):
            logger.info("Checking in {0} seconds. Attempt {1}/{2}".format(sleep_duration, i, attempts))
            time.sleep(sleep_duration)

            try:
                response = dns.resolver.query(qname='_acme-challenge' + '.' + domain, rdtype='TXT')

                for rdata in response:
                    logger.info("Found record: {0} IN TXT {1} found".format(
                        rdata.to_text(), '_acme-challenge' + '.' + domain))

                    if rdata.to_text() == txt_record:
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
                            'TTL': 60,
                            'ResourceRecords': [{'Value': txt_record}]
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
                            'TTL': 60,
                            'ResourceRecords': [{'Value': txt_record}]
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
    def __init__(self):
        self.provider = "azure"

    def create_dns_record(self, domain, txt_record):
        logger.info("create_dns_record")
        raise ManualeError(NotImplementedError)

    def delete_dns_record(self, domain, txt_record):
        logger.info("delete_dns_record")
        raise ManualeError(NotImplementedError)