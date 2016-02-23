from __future__ import print_function
"""
S3 Tubing extension.
"""

import boto3
import logging
from tubing import sources, sinks, compat

logger = logging.getLogger('tubing.ext.s3')


@compat.python_2_unicode_compatible
class S3Reader(object):  # pragma: no cover
    """
    Read file from S3. Expects AWS environmental variables to be set.
    """

    def __init__(self, bucket, key):  # pragma: no cover
        """
        Create an S3 Source stream.
        """
        s3 = boto3.client('s3')
        self.bucket = bucket
        self.key = key
        self.response = s3.get_object(Bucket=bucket, Key=key)

    def read(self, amt):
        r = self.response['Body'].read(amt)
        return r or b'', not r

    def __str__(self):
        return "<tubing.ext.s3.S3Source s3://%s/%s>" % (self.bucket, self.key)


S3Source = sources.MakeSourceFactory(S3Reader)


class S3MultipartWriter(object):  # pragma: no cover
    """
    Send file to S3. Expects AWS environmental variables to be set.
    If the file is too small, this will fail. Have your apparatus
    write to a Bytes sink instead, and use client.put_object(Body=sink)
    after the tubes have executed.
    """

    def __init__(self, bucket, key):
        """
        Initiate the multi-part upload.
        """
        self.s3 = boto3.client('s3')
        self.bucket = bucket
        self.key = key
        logger.debug("Creating upload for %s %s" % (bucket, key))
        upload = self.s3.create_multipart_upload(Bucket=bucket, Key=key)
        self.upload_id = upload["UploadId"]
        # start with 1
        self.part_number = 1
        self.part_info = dict(Parts=[])

    def track_part(self, part_number, part):
        """
        Keep track of all parts so we can finalize multipart upload.
        """
        self.part_info['Parts'].append(
            dict(
                PartNumber=part_number,
                ETag=part['ETag'],
            )
        )

    def write(self, chunk):
        """
        Upload a part.
        """
        logger.debug("Posting " + self.part_number)
        part = self.s3.upload_part(
            Bucket=self.bucket,
            Key=self.key,
            PartNumber=self.part_number,
            UploadId=self.upload_id,
            Body=chunk,
        )
        self.track_part(self.part_number, part)
        self.part_number += 1

    def close(self):
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


S3Multipart = sinks.MakeSinkFactory(S3MultipartWriter)
