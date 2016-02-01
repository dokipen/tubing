from __future__ import print_function
"""
S3 Tubing extension.
"""

import boto3
import logging


logger = logging.getLogger('tubing.ext.s3')


def S3Source(bucket, key):
    """
    Create an S3 Source stream.
    """
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket, Key=key)
    return response['Body']


class S3Sink(object):
    """
    Send MediaDocs to S3. Expects AWS environmental variables to be set.
    """
    def __init__(self, bucket, key):
        """
        Initiate the multipart upload.
        """
        self.s3 = boto3.client('s3')
        self.bucket = bucket
        self.key = key
        logger.debug("Creating upload for {} {}".format(bucket, key))
        upload = self.s3.create_multipart_upload(Bucket=bucket, Key=key)
        self.upload_id = upload["UploadId"]
        # start with 1
        self.part_number = 1
        self.part_info = dict(Parts=[])

    def track_part(self, part_number, part):
        """
        Keep track of all parts so we can finalize multipart upload.
        """
        self.part_info['Parts'].append(dict(
            PartNumber=part_number,
            ETag=part['ETag'],
        ))

    def write(self, chunk):
        """
        Upload a part.
        """
        logger.debug("Posting {}".format(self.part_number))
        part = self.s3.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            PartNumber=self.part_number,
            UploadId=self.upload_id,
            Body=chunk,
        )
        self.track_part(self.part_number, part)
        self.part_number += 1

    def done(self):
        """
        Finalize upload.
        """
        self.s3.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload=self.part_info,
        )

    def abort(self):
        """
        Something failed, abort!
        """
        self.s3.abort_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
        )
