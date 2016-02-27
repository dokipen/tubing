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


class MultipartWriter(object):  # pragma: no cover
    """
    Send file to S3. Expects AWS environmental variables to be set.
    """

    def __init__(self, bucket, key):
        """
        Initiate the multi-part upload.
        """
        self.s3 = boto3.client('s3')
        self.bucket = bucket
        self.key = key
        logger.debug("Creating upload for %s %s", bucket, key)
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
        if len(chunk):
            logger.debug("Posting %s [%d]", self.part_number, len(chunk))
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
        if self.part_number > 1:
            self.s3.complete_multipart_upload(
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.upload_id,
                MultipartUpload=self.part_info,
            )
        else:
            logger.warn("s3.MultipartUploader [%s/%s] got empty " \
                        "set, no file created",
                self.bucket,
                self.key,
            )
            self.abort()

    def abort(self):
        """
        Something failed, abort!
        """
        self.s3.abort_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
        )


MultipartUpload = sinks.MakeSinkFactory(MultipartWriter)
